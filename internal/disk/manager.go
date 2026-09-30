package disk

import (
	"bytes"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	unitDir                 = "/etc/systemd/system"
	unitPrefix              = "storage-mount-"
	rcloneRemote            = "remote"
	rcloneBin               = "/usr/bin/rclone"
	defaultQuotaGB          = 5
	minQuotaGB              = 2
	reservedFreeGB          = 15
	defaultRcloneConfigPath = "/root/.config/rclone/rclone.conf"
	defaultRcPortBase       = 5572
	rcPortRangeSize         = 1000
	configFileMode          = 0o600
	rcTimeout               = 5 * time.Second
	maxRcAnswer             = 1 << 20
	defaultFlushLimit       = 30 * time.Minute
	unmountGap              = 5 * time.Minute
	defaultSettle           = time.Second
)

var ErrFlushing = errors.New("disk cache is still uploading")

//go:embed unit.tmpl
var unitTmpl string

var unitTemplate = template.Must(template.New("unit").Parse(unitTmpl))

type unitParams struct {
	DriveID   string
	EnvFile   string
	Command   string
	MountPath string
}

type Uploads struct {
	InProgress int
	Queued     int
	Errored    int
	OutOfSpace bool
	Sent       int64
	Completed  int64
	Errors     int64
	LastError  string
}

func (u Uploads) Pending() int { return u.InProgress + u.Queued }

type rcAccess struct {
	addr string
	user string
	pass string
}

type unmountWait struct {
	since time.Time
	seen  time.Time
}

type Manager struct {
	systemd          system.Systemd
	exec             system.Executor
	log              *slog.Logger
	unitsDir         string
	rcloneConfigPath string
	rcPortBase       int
	httpClient       *http.Client
	direct           bool
	directMarkersDir string
	flushLimit       time.Duration
	settle           time.Duration

	mu         sync.Mutex
	unmounting map[string]*unmountWait
}

func NewManager(systemd system.Systemd, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{
		systemd:          systemd,
		exec:             exec,
		log:              log,
		unitsDir:         unitDir,
		rcloneConfigPath: defaultRcloneConfigPath,
		rcPortBase:       defaultRcPortBase,
		httpClient:       &http.Client{Timeout: rcTimeout},
		directMarkersDir: "/var/lib/agent/mounts",
		flushLimit:       defaultFlushLimit,
		settle:           defaultSettle,
		unmounting:       map[string]*unmountWait{},
	}
}

func (m *Manager) SetUnitsDir(dir string) { m.unitsDir = dir }

func (m *Manager) SetRcloneConfigPath(p string) { m.rcloneConfigPath = p }

func (m *Manager) SetRcPortBase(p int) {
	if p > 0 {
		m.rcPortBase = p
	}
}

func (m *Manager) SetDirectMode(enabled bool) { m.direct = enabled }

func (m *Manager) SetDirectMarkersDir(dir string) { m.directMarkersDir = dir }

func (m *Manager) mountArgs(spec client.AgentDiskSpec, quotaGB int) []string {
	return []string{
		"mount", fmt.Sprintf("%s:%s/%s", rcloneRemote, spec.Bucket, spec.S3Path), spec.MountPath,
		"--config", m.rcloneConfigPath,
		"--vfs-cache-mode", "full",
		"--vfs-cache-max-size", strconv.Itoa(quotaGB) + "G",
		"--vfs-cache-max-age", "24h",
		"--vfs-cache-min-free-space", strconv.Itoa(reservedFreeGB) + "G",
		"--dir-cache-time", "1m",
		"--vfs-read-chunk-size", "4M",
		"--vfs-read-chunk-streams", "32",
		"--transfers", "4",
		"--s3-upload-concurrency", "8",
		"--s3-chunk-size", "16M",
		"--allow-other",
		"--daemon-timeout", "10m",
	}
}

func (m *Manager) Mount(ctx context.Context, spec client.AgentDiskSpec) error {
	if err := os.MkdirAll(spec.MountPath, 0o777); err != nil {
		return fmt.Errorf("mkdir mount path: %w", err)
	}
	if err := os.Chmod(spec.MountPath, 0o777); err != nil {
		return fmt.Errorf("chmod mount path: %w", err)
	}

	args := m.mountArgs(spec, m.perDriveQuotaGB(ctx))
	if m.direct {
		return m.mountDirect(ctx, spec, args)
	}

	envFile, err := m.ensureRc(spec.ID)
	if err != nil {
		return fmt.Errorf("rc access: %w", err)
	}
	params := unitParams{
		DriveID:   spec.ID,
		EnvFile:   envFile,
		Command:   rcloneBin + " " + strings.Join(slices.Concat(args, []string{"--rc"}), " "),
		MountPath: spec.MountPath,
	}

	var buf bytes.Buffer
	if err := unitTemplate.Execute(&buf, params); err != nil {
		return fmt.Errorf("render unit: %w", err)
	}

	unitName := unitNameFor(spec.ID)
	unitPath := filepath.Join(m.unitsDir, unitName)

	existing, _ := os.ReadFile(unitPath)
	if !bytes.Equal(existing, buf.Bytes()) {
		if err := os.WriteFile(unitPath, buf.Bytes(), 0o644); err != nil {
			return fmt.Errorf("write unit %s: %w", unitPath, err)
		}
		if err := m.systemd.DaemonReload(ctx); err != nil {
			return fmt.Errorf("daemon-reload: %w", err)
		}
	}
	if err := m.systemd.Enable(ctx, unitName); err != nil {
		m.log.Warn("systemctl enable failed", "unit", unitName, "err", err)
	}
	if err := m.systemd.Start(ctx, unitName); err != nil {
		return fmt.Errorf("start %s: %w", unitName, err)
	}

	time.Sleep(m.settle)
	active, err := m.systemd.IsActive(ctx, unitName)
	if err != nil {
		return fmt.Errorf("is-active check: %w", err)
	}
	if !active {
		return fmt.Errorf("unit %s did not become active", unitName)
	}
	return nil
}

func (m *Manager) mountDirect(ctx context.Context, spec client.AgentDiskSpec, args []string) error {
	if err := os.MkdirAll(m.directMarkersDir, 0o755); err != nil {
		return fmt.Errorf("mkdir markers dir: %w", err)
	}
	if _, err := m.exec.Run(ctx, 30*time.Second, rcloneBin, slices.Concat(args, []string{"--daemon"})...); err != nil {
		return fmt.Errorf("rclone mount: %w", err)
	}

	markerPath := filepath.Join(m.directMarkersDir, spec.ID)
	if err := os.WriteFile(markerPath, []byte(spec.MountPath), 0o644); err != nil {
		m.log.Warn("write direct marker failed", "id", spec.ID, "err", err)
	}

	for i := 0; i < 20; i++ {
		if _, err := m.exec.Run(ctx, 2*time.Second, "mountpoint", "-q", spec.MountPath); err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("mount %s did not become active within timeout", spec.MountPath)
}

func (m *Manager) Unmount(ctx context.Context, driveID string) error {
	if m.direct {
		return m.unmountDirect(ctx, driveID)
	}
	unitName := unitNameFor(driveID)
	unitPath := filepath.Join(m.unitsDir, unitName)

	active, err := m.systemd.IsActive(ctx, unitName)
	if err != nil {
		m.log.Warn("is-active check failed during unmount", "unit", unitName, "err", err)
	}
	if active {
		if m.flushing(ctx, driveID) {
			return ErrFlushing
		}
		if err := m.systemd.Stop(ctx, unitName); err != nil {
			return fmt.Errorf("stop %s: %w", unitName, err)
		}
	}
	m.mu.Lock()
	delete(m.unmounting, driveID)
	m.mu.Unlock()
	if err := m.systemd.Disable(ctx, unitName); err != nil {
		m.log.Debug("systemctl disable returned error", "unit", unitName, "err", err)
	}

	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove unit file: %w", err)
	}
	if err := os.Remove(m.rcEnvPath(driveID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove rc env file: %w", err)
	}

	if err := m.systemd.DaemonReload(ctx); err != nil {
		return fmt.Errorf("daemon-reload after unmount: %w", err)
	}
	return nil
}

func (m *Manager) flushing(ctx context.Context, driveID string) bool {
	now := time.Now()
	m.mu.Lock()
	w := m.unmounting[driveID]
	if w == nil || now.Sub(w.seen) > unmountGap {
		w = &unmountWait{since: now}
		m.unmounting[driveID] = w
	}
	w.seen = now
	since := w.since
	m.mu.Unlock()

	if now.Sub(since) >= m.flushLimit {
		m.log.Warn("disk uploads did not finish in time, unmounting anyway", "id", driveID, "limit", m.flushLimit.String())
		return false
	}
	up, err := m.Uploads(ctx, driveID)
	if err != nil {
		m.log.Warn("disk uploads unknown, keeping the disk mounted", "id", driveID, "err", err)
		return true
	}
	if up.Pending() > 0 {
		m.log.Info("waiting for disk uploads before unmount", "id", driveID, "pending", up.Pending())
		return true
	}
	return false
}

func (m *Manager) unmountDirect(ctx context.Context, driveID string) error {
	markerPath := filepath.Join(m.directMarkersDir, driveID)
	mountPath, readErr := os.ReadFile(markerPath)
	if readErr != nil {
		return nil
	}
	if _, err := m.exec.Run(ctx, 10*time.Second, "fusermount", "-uz", strings.TrimSpace(string(mountPath))); err != nil {
		m.log.Warn("fusermount returned error (continuing)", "err", err)
	}
	if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove direct marker: %w", err)
	}
	return nil
}

func (m *Manager) ListUnits() ([]string, error) {
	if m.direct {
		entries, err := os.ReadDir(m.directMarkersDir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		var ids []string
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			ids = append(ids, e.Name())
		}
		return ids, nil
	}

	entries, err := os.ReadDir(m.unitsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, unitPrefix) || !strings.HasSuffix(name, ".service") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, unitPrefix), ".service")
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (m *Manager) IsActive(ctx context.Context, driveID string) (bool, error) {
	if m.direct {
		markerPath := filepath.Join(m.directMarkersDir, driveID)
		mountPath, err := os.ReadFile(markerPath)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		_, err = m.exec.Run(ctx, 2*time.Second, "mountpoint", "-q", strings.TrimSpace(string(mountPath)))
		return err == nil, nil
	}
	return m.systemd.IsActive(ctx, unitNameFor(driveID))
}

func (m *Manager) MountID(ctx context.Context, driveID string) (string, error) {
	if m.direct {
		return "", nil
	}
	return m.systemd.InvocationID(ctx, unitNameFor(driveID))
}

func (m *Manager) EnsureRunning(ctx context.Context, driveID string) error {
	if m.direct {
		return nil
	}
	unit := unitNameFor(driveID)
	if active, err := m.systemd.IsActive(ctx, unit); err == nil && active {
		return nil
	}
	return m.systemd.Start(ctx, unit)
}

func (m *Manager) ApplyCredentials(ctx context.Context, creds *client.StorageCredentials) error {
	if creds == nil {
		return fmt.Errorf("apply credentials: creds is nil")
	}
	if err := m.writeRcloneConfig(creds); err != nil {
		return fmt.Errorf("write rclone config: %w", err)
	}

	ids, err := m.ListUnits()
	if err != nil {
		return fmt.Errorf("list units for reload: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}
	if m.direct {
		m.log.Info("ApplyCredentials: direct mode, rclone.conf written; mounts re-read on next op", "count", len(ids))
		return nil
	}

	hotReloaded := 0
	restarted := 0
	for _, id := range ids {
		if err := m.rcReload(ctx, id, creds); err != nil {
			m.log.Warn("rc reload failed, falling back to restart", "id", id, "err", err)
			if rerr := m.restartUnit(ctx, id); rerr != nil {
				return fmt.Errorf("restart fallback for %s: %w", id, rerr)
			}
			restarted++
		} else {
			m.log.Debug("hot-reloaded creds via rc", "id", id)
			hotReloaded++
		}
	}
	m.log.Info("ApplyCredentials done", "hot_reloaded", hotReloaded, "restarted", restarted, "total", len(ids))
	return nil
}

func (m *Manager) writeRcloneConfig(c *client.StorageCredentials) error {
	dir := filepath.Dir(m.rcloneConfigPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	body := fmt.Sprintf(`[%s]
type = s3
provider = Other
env_auth = false
access_key_id = %s
secret_access_key = %s
endpoint = %s
force_path_style = false
acl = private
`, rcloneRemote, c.AccessKey, c.SecretKey, c.Endpoint)

	tmp := m.rcloneConfigPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), configFileMode); err != nil {
		return err
	}
	return os.Rename(tmp, m.rcloneConfigPath)
}

func (m *Manager) rcReload(ctx context.Context, driveID string, creds *client.StorageCredentials) error {
	body := map[string]any{
		"name": rcloneRemote,
		"parameters": map[string]string{
			"type":              "s3",
			"provider":          "Other",
			"env_auth":          "false",
			"access_key_id":     creds.AccessKey,
			"secret_access_key": creds.SecretKey,
			"endpoint":          creds.Endpoint,
			"force_path_style":  "false",
			"acl":               "private",
		},
	}
	return m.rc(ctx, driveID, "config/update", body, nil)
}

func (m *Manager) Uploads(ctx context.Context, driveID string) (Uploads, error) {
	if m.direct {
		return Uploads{}, nil
	}
	var vfs struct {
		DiskCache *struct {
			UploadsInProgress int  `json:"uploadsInProgress"`
			UploadsQueued     int  `json:"uploadsQueued"`
			ErroredFiles      int  `json:"erroredFiles"`
			OutOfSpace        bool `json:"outOfSpace"`
		} `json:"diskCache"`
	}
	if err := m.rc(ctx, driveID, "vfs/stats", nil, &vfs); err != nil {
		return Uploads{}, err
	}
	var core struct {
		Bytes     int64  `json:"bytes"`
		Transfers int64  `json:"transfers"`
		Errors    int64  `json:"errors"`
		LastError string `json:"lastError"`
	}
	if err := m.rc(ctx, driveID, "core/stats", map[string]bool{"short": true}, &core); err != nil {
		return Uploads{}, err
	}
	up := Uploads{Sent: core.Bytes, Completed: core.Transfers, Errors: core.Errors, LastError: core.LastError}
	if d := vfs.DiskCache; d != nil {
		up.InProgress, up.Queued, up.Errored, up.OutOfSpace = d.UploadsInProgress, d.UploadsQueued, d.ErroredFiles, d.OutOfSpace
	}
	return up, nil
}

func (m *Manager) rc(ctx context.Context, driveID, method string, in, out any) error {
	acc, err := m.readRc(driveID)
	if err != nil {
		return fmt.Errorf("rc access: %w", err)
	}
	body := []byte("{}")
	if in != nil {
		if body, err = json.Marshal(in); err != nil {
			return fmt.Errorf("rc %s: %w", method, err)
		}
	}
	rctx, cancel := context.WithTimeout(ctx, rcTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, "http://"+acc.addr+"/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(acc.user, acc.pass)
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("rc %s: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rc %s: status %d", method, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRcAnswer)).Decode(out); err != nil {
		return fmt.Errorf("rc %s: %w", method, err)
	}
	return nil
}

func (m *Manager) rcEnvPath(driveID string) string {
	return filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-"+driveID+".env")
}

func (m *Manager) readRc(driveID string) (rcAccess, error) {
	path := m.rcEnvPath(driveID)
	raw, err := os.ReadFile(path)
	if err != nil {
		return rcAccess{}, err
	}
	var acc rcAccess
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, _ := strings.Cut(strings.TrimSpace(line), "=")
		switch key {
		case "RCLONE_RC_ADDR":
			acc.addr = value
		case "RCLONE_RC_USER":
			acc.user = value
		case "RCLONE_RC_PASS":
			acc.pass = value
		}
	}
	if acc.addr == "" || acc.user == "" || acc.pass == "" {
		return rcAccess{}, fmt.Errorf("incomplete rc access in %s", path)
	}
	return acc, nil
}

func (m *Manager) ensureRc(driveID string) (string, error) {
	path := m.rcEnvPath(driveID)
	if _, err := m.readRc(driveID); err == nil {
		return path, nil
	}
	port, err := m.freeRcPort()
	if err != nil {
		return "", err
	}
	user, err := randomHex(16)
	if err != nil {
		return "", err
	}
	pass, err := randomHex(32)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	body := fmt.Sprintf("RCLONE_RC_ADDR=127.0.0.1:%d\nRCLONE_RC_USER=%s\nRCLONE_RC_PASS=%s\n", port, user, pass)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), configFileMode); err != nil {
		return "", err
	}
	if err := os.Chmod(tmp, configFileMode); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

func (m *Manager) freeRcPort() (int, error) {
	taken := map[int]bool{}
	files, _ := filepath.Glob(filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-*.env"))
	for _, f := range files {
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "rc-"), ".env")
		acc, err := m.readRc(id)
		if err != nil {
			continue
		}
		if _, p, err := net.SplitHostPort(acc.addr); err == nil {
			if n, err := strconv.Atoi(p); err == nil {
				taken[n] = true
			}
		}
	}
	for port := m.rcPortBase; port < m.rcPortBase+rcPortRangeSize; port++ {
		if taken[port] {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			continue
		}
		_ = l.Close()
		return port, nil
	}
	return 0, fmt.Errorf("no free rc port in %d-%d", m.rcPortBase, m.rcPortBase+rcPortRangeSize-1)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (m *Manager) restartUnit(ctx context.Context, driveID string) error {
	unit := unitNameFor(driveID)
	if err := m.systemd.Restart(ctx, unit); err != nil {
		return fmt.Errorf("restart %s: %w", unit, err)
	}
	time.Sleep(2 * m.settle)
	active, err := m.systemd.IsActive(ctx, unit)
	if err != nil || !active {
		return fmt.Errorf("unit %s did not come back active after restart (err=%v)", unit, err)
	}
	return nil
}

func unitNameFor(driveID string) string { return unitPrefix + driveID + ".service" }

var dfFreeGB = regexp.MustCompile(`(\d+)G`)

func (m *Manager) perDriveQuotaGB(ctx context.Context) int {
	out, err := m.exec.Run(ctx, 5*time.Second, "df", "-BG", "/")
	if err != nil {
		m.log.Warn("df failed, using default quota", "err", err)
		return defaultQuotaGB
	}
	lines := strings.Split(out, "\n")
	if len(lines) < 2 {
		return defaultQuotaGB
	}
	fields := strings.Fields(lines[1])
	if len(fields) < 4 {
		return defaultQuotaGB
	}
	m2 := dfFreeGB.FindStringSubmatch(fields[3])
	if len(m2) < 2 {
		return defaultQuotaGB
	}
	free, err := strconv.Atoi(m2[1])
	if err != nil {
		return defaultQuotaGB
	}
	total := free - reservedFreeGB
	if total < minQuotaGB {
		total = minQuotaGB
	}

	ids, _ := m.ListUnits()
	count := len(ids)
	if count < 1 {
		count = 1
	}
	per := total / count
	if per < minQuotaGB {
		per = minQuotaGB
	}
	return per
}

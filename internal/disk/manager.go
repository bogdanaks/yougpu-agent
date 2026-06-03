package disk

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	unitDir                 = "/etc/systemd/system"
	unitPrefix              = "storage-mount-"
	rcloneRemote            = "remote"
	defaultQuotaGB          = 5
	minQuotaGB              = 2
	reservedFreeGB          = 15
	defaultRcloneConfigPath = "/root/.config/rclone/rclone.conf"
	defaultRcPortBase       = 5572
	rcPortRangeSize         = 1000
	configFileMode          = 0o600
	rcReloadTimeout         = 5 * time.Second
)

//go:embed unit.tmpl
var unitTmpl string

var unitTemplate = template.Must(template.New("unit").Parse(unitTmpl))

type unitParams struct {
	DriveID   string
	Remote    string
	Bucket    string
	S3Path    string
	MountPath string
	QuotaGB   int
	RcPort    int
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
}

func NewManager(systemd system.Systemd, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{
		systemd:          systemd,
		exec:             exec,
		log:              log,
		unitsDir:         unitDir,
		rcloneConfigPath: defaultRcloneConfigPath,
		rcPortBase:       defaultRcPortBase,
		httpClient:       &http.Client{Timeout: rcReloadTimeout},
		directMarkersDir: "/var/lib/agent/mounts",
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

func (m *Manager) Mount(ctx context.Context, spec client.AgentDiskSpec) error {
	if err := os.MkdirAll(spec.MountPath, 0o777); err != nil {
		return fmt.Errorf("mkdir mount path: %w", err)
	}
	if err := os.Chmod(spec.MountPath, 0o777); err != nil {
		return fmt.Errorf("chmod mount path: %w", err)
	}

	if m.direct {
		return m.mountDirect(ctx, spec)
	}

	quota := m.perDriveQuotaGB(ctx)
	params := unitParams{
		DriveID:   spec.ID,
		Remote:    rcloneRemote,
		Bucket:    spec.Bucket,
		S3Path:    spec.S3Path,
		MountPath: spec.MountPath,
		QuotaGB:   quota,
		RcPort:    m.RcPortFor(spec.ID),
	}

	var buf bytes.Buffer
	if err := unitTemplate.Execute(&buf, params); err != nil {
		return fmt.Errorf("render unit: %w", err)
	}

	unitName := unitNameFor(spec.ID)
	unitPath := filepath.Join(m.unitsDir, unitName)

	existing, _ := os.ReadFile(unitPath)
	changed := !bytes.Equal(existing, buf.Bytes())
	if changed {
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

	time.Sleep(1 * time.Second)
	active, err := m.systemd.IsActive(ctx, unitName)
	if err != nil {
		return fmt.Errorf("is-active check: %w", err)
	}
	if !active {
		return fmt.Errorf("unit %s did not become active", unitName)
	}
	return nil
}

func (m *Manager) mountDirect(ctx context.Context, spec client.AgentDiskSpec) error {
	if err := os.MkdirAll(m.directMarkersDir, 0o755); err != nil {
		return fmt.Errorf("mkdir markers dir: %w", err)
	}

	source := fmt.Sprintf("%s:%s/%s", rcloneRemote, spec.Bucket, spec.S3Path)
	args := []string{
		"mount", source, spec.MountPath,
		"--config", m.rcloneConfigPath,
		"--vfs-cache-mode", "writes",
		"--allow-other",
		"--daemon",
		"--daemon-timeout", "10m",
	}
	if _, err := m.exec.Run(ctx, 30*time.Second, "rclone", args...); err != nil {
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
		if err := m.systemd.Stop(ctx, unitName); err != nil {
			return fmt.Errorf("stop %s: %w", unitName, err)
		}
	}
	if err := m.systemd.Disable(ctx, unitName); err != nil {
		m.log.Debug("systemctl disable returned error", "unit", unitName, "err", err)
	}

	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove unit file: %w", err)
	}

	if err := m.systemd.DaemonReload(ctx); err != nil {
		return fmt.Errorf("daemon-reload after unmount: %w", err)
	}
	return nil
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
		port := m.RcPortFor(id)
		if err := m.rcReload(ctx, port, creds); err != nil {
			m.log.Warn("rc reload failed, falling back to restart", "id", id, "port", port, "err", err)
			if rerr := m.restartUnit(ctx, id); rerr != nil {
				return fmt.Errorf("restart fallback for %s: %w", id, rerr)
			}
			restarted++
		} else {
			m.log.Debug("hot-reloaded creds via rc", "id", id, "port", port)
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
provider = Minio
env_auth = false
access_key_id = %s
secret_access_key = %s
session_token = %s
endpoint = %s
acl = private
`, rcloneRemote, c.AccessKey, c.SecretKey, c.SessionToken, c.Endpoint)

	tmp := m.rcloneConfigPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), configFileMode); err != nil {
		return err
	}
	return os.Rename(tmp, m.rcloneConfigPath)
}

func (m *Manager) rcReload(ctx context.Context, port int, creds *client.StorageCredentials) error {
	body := map[string]any{
		"name": rcloneRemote,
		"parameters": map[string]string{
			"type":              "s3",
			"provider":          "Minio",
			"env_auth":          "false",
			"access_key_id":     creds.AccessKey,
			"secret_access_key": creds.SecretKey,
			"session_token":     creds.SessionToken,
			"endpoint":          creds.Endpoint,
			"acl":               "private",
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal rc body: %w", err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/config/update", port)
	rctx, cancel := context.WithTimeout(ctx, rcReloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("rc dial: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rc status %d", resp.StatusCode)
	}
	return nil
}

func (m *Manager) restartUnit(ctx context.Context, driveID string) error {
	unit := unitNameFor(driveID)
	if err := m.systemd.Restart(ctx, unit); err != nil {
		return fmt.Errorf("restart %s: %w", unit, err)
	}
	time.Sleep(2 * time.Second)
	active, err := m.systemd.IsActive(ctx, unit)
	if err != nil || !active {
		return fmt.Errorf("unit %s did not come back active after restart (err=%v)", unit, err)
	}
	return nil
}

func (m *Manager) RcPortFor(driveID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(driveID))
	return m.rcPortBase + int(h.Sum32()%rcPortRangeSize)
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

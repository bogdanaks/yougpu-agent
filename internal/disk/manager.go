package disk

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"log/slog"
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
	unitDir        = "/etc/systemd/system"
	unitPrefix     = "storage-mount-"
	rcloneRemote   = "remote"
	defaultQuotaGB = 5
	minQuotaGB     = 2
	reservedFreeGB = 15
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
}

type Manager struct {
	systemd  system.Systemd
	exec     system.Executor
	log      *slog.Logger
	unitsDir string
}

func NewManager(systemd system.Systemd, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{
		systemd:  systemd,
		exec:     exec,
		log:      log,
		unitsDir: unitDir,
	}
}

func (m *Manager) SetUnitsDir(dir string) { m.unitsDir = dir }

func (m *Manager) Mount(ctx context.Context, spec client.AgentDiskSpec) error {
	if err := os.MkdirAll(spec.MountPath, 0o777); err != nil {
		return fmt.Errorf("mkdir mount path: %w", err)
	}
	if err := os.Chmod(spec.MountPath, 0o777); err != nil {
		return fmt.Errorf("chmod mount path: %w", err)
	}

	quota := m.perDriveQuotaGB(ctx)
	params := unitParams{
		DriveID:   spec.ID,
		Remote:    rcloneRemote,
		Bucket:    spec.Bucket,
		S3Path:    spec.S3Path,
		MountPath: spec.MountPath,
		QuotaGB:   quota,
	}

	var buf bytes.Buffer
	if err := unitTemplate.Execute(&buf, params); err != nil {
		return fmt.Errorf("render unit: %w", err)
	}

	unitName := unitNameFor(spec.ID)
	unitPath := filepath.Join(m.unitsDir, unitName)
	if err := os.WriteFile(unitPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write unit %s: %w", unitPath, err)
	}

	if err := m.systemd.DaemonReload(ctx); err != nil {
		return fmt.Errorf("daemon-reload: %w", err)
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

func (m *Manager) Unmount(ctx context.Context, driveID string) error {
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

func (m *Manager) ListUnits() ([]string, error) {
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
	return m.systemd.IsActive(ctx, unitNameFor(driveID))
}

// RestartAll restarts all storage-mount units one at a time, waiting for each to come
// back active before touching the next. Called after credentials rotation.
func (m *Manager) RestartAll(ctx context.Context) error {
	ids, err := m.ListUnits()
	if err != nil {
		return err
	}
	for _, id := range ids {
		unit := unitNameFor(id)
		if err := m.systemd.Restart(ctx, unit); err != nil {
			return fmt.Errorf("restart %s: %w", unit, err)
		}
		time.Sleep(2 * time.Second)
		active, err := m.systemd.IsActive(ctx, unit)
		if err != nil || !active {
			return fmt.Errorf("unit %s did not come back active after restart (err=%v)", unit, err)
		}
	}
	return nil
}

func unitNameFor(driveID string) string { return unitPrefix + driveID + ".service" }

var dfFreeGB = regexp.MustCompile(`(\d+)G`)

// perDriveQuotaGB splits (free - reserved) across all mounted drives, floored at minQuotaGB.
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

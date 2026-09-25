package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	sentinelFile      = "lifecycle_state"
	storageUnitPrefix = "storage-mount-"

	StateAlive          = "alive"
	StateSyncing        = "syncing"
	StateSynced         = "synced"
	StateDestroyingSelf = "destroying_self"

	dockerStopTimeout = 30 * time.Second
	dockerKillTimeout = 15 * time.Second
	dockerListTimeout = 5 * time.Second
)

type Disker interface {
	ListUnits() ([]string, error)
	PendingUploads(ctx context.Context, id string) (int, error)
}

type Hooks struct {
	BeforeStop func(context.Context)
	AfterStop  func(ctx context.Context, stopErr error) bool
}

type SystemdStopper interface {
	Stop(ctx context.Context, unit string) error
	Poweroff(ctx context.Context) error
}

type Manager struct {
	stateDir string
	systemd  SystemdStopper
	exec     system.Executor
	log      *slog.Logger
}

func NewManager(stateDir string, systemd SystemdStopper, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{stateDir: stateDir, systemd: systemd, exec: exec, log: log}
}

func (m *Manager) CurrentState() string {
	raw, err := os.ReadFile(m.path())
	if err != nil {
		return StateAlive
	}
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return StateAlive
	}
	return s
}

func (m *Manager) SetState(state string) error {
	tmp := m.path() + ".tmp"
	if err := os.WriteFile(tmp, []byte(state), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path())
}

func (m *Manager) HandleTermination(ctx context.Context, disker Disker, hooks Hooks) (string, error) {
	switch m.CurrentState() {
	case StateSynced, StateDestroyingSelf:
		return StateSynced, nil
	case StateSyncing:
	default:
		if err := m.SetState(StateSyncing); err != nil {
			return StateAlive, fmt.Errorf("persist syncing state: %w", err)
		}
		if hooks.BeforeStop != nil {
			hooks.BeforeStop(ctx)
		}
	}
	stopErr := m.stopContainers(ctx)
	if stopErr != nil {
		m.log.Warn("containers did not stop", "err", stopErr)
	}
	if hooks.AfterStop != nil && !hooks.AfterStop(ctx, stopErr) {
		return StateSyncing, nil
	}
	return m.finishSync(ctx, disker)
}

func (m *Manager) finishSync(ctx context.Context, disker Disker) (string, error) {
	pending, err := m.pendingUploads(ctx, disker)
	if err != nil {
		return StateSyncing, err
	}
	if pending > 0 {
		m.log.Info("waiting for storage uploads before unmount", "pending", pending)
		return StateSyncing, nil
	}
	if err := m.flushStorageUnits(ctx, disker); err != nil {
		return StateSyncing, err
	}
	if err := m.SetState(StateSynced); err != nil {
		return StateSynced, fmt.Errorf("persist synced state: %w", err)
	}
	return StateSynced, nil
}

func (m *Manager) pendingUploads(ctx context.Context, disker Disker) (int, error) {
	ids, err := disker.ListUnits()
	if err != nil {
		return 0, fmt.Errorf("list units: %w", err)
	}
	total := 0
	for _, id := range ids {
		n, err := disker.PendingUploads(ctx, id)
		if err != nil {
			return 0, fmt.Errorf("uploads of %s: %w", id, err)
		}
		total += n
	}
	return total, nil
}

func (m *Manager) Poweroff(ctx context.Context) error {
	if err := m.SetState(StateDestroyingSelf); err != nil {
		m.log.Warn("could not persist destroying_self state", "err", err)
	}
	return m.systemd.Poweroff(ctx)
}

func (m *Manager) stopContainers(ctx context.Context) error {
	if _, err := m.exec.Run(ctx, dockerListTimeout, "sh", "-c", "command -v docker"); err != nil {
		return nil
	}
	ids, err := m.running(ctx)
	if err != nil || len(ids) == 0 {
		return err
	}
	if _, err := m.exec.Run(ctx, dockerStopTimeout+10*time.Second, "docker", append([]string{"stop", "-t", "30"}, ids...)...); err != nil {
		m.log.Warn("docker stop returned error", "err", err)
	}
	if ids, err = m.running(ctx); err != nil || len(ids) == 0 {
		return err
	}
	if _, err := m.exec.Run(ctx, dockerKillTimeout, "docker", append([]string{"kill"}, ids...)...); err != nil {
		m.log.Warn("docker kill returned error", "err", err)
	}
	if ids, err = m.running(ctx); err != nil || len(ids) == 0 {
		return err
	}
	return fmt.Errorf("контейнер не остановился: %s", strings.Join(ids, " "))
}

func (m *Manager) running(ctx context.Context) ([]string, error) {
	out, err := m.exec.Run(ctx, dockerListTimeout, "docker", "ps", "-q")
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w", err)
	}
	return strings.Fields(out), nil
}

func (m *Manager) flushStorageUnits(ctx context.Context, disker Disker) error {
	ids, err := disker.ListUnits()
	if err != nil {
		return fmt.Errorf("list units: %w", err)
	}
	var errs []error
	for _, id := range ids {
		unit := storageUnitPrefix + id + ".service"
		if err := m.systemd.Stop(ctx, unit); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", unit, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (m *Manager) path() string {
	return filepath.Join(m.stateDir, sentinelFile)
}

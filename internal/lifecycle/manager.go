// Package lifecycle обслуживает state machine агент'а:
//   alive → (видит deletion_requested_at) → syncing → (после flush) → synced → poweroff.
// Состояние пишется на диск (sentinel file) — после рестарта агента в середине sync'а
// мы не начинаем заново, а продолжаем с того же шага.
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
	sentinelFile = "lifecycle_state"

	StateAlive          = "alive"
	StateSyncing        = "syncing"
	StateSynced         = "synced"
	StateDestroyingSelf = "destroying_self"

	dockerStopTimeout = 30 * time.Second
)

// Disker — то, что Manager.Sync должен знать про диски: stop'нуть rclone unit'ы чтобы
// flush'нуть VFS-кэш в S3. Совпадает с подмножеством disk.Manager.
type Disker interface {
	ListUnits() ([]string, error)
}

// SystemdStopper — то, что нужно для stop'а unit'ов и финального poweroff'а.
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

// CurrentState reads the sentinel; defaults to "alive" when missing.
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

// SetState atomically writes a new lifecycle state to the sentinel.
func (m *Manager) SetState(state string) error {
	tmp := m.path() + ".tmp"
	if err := os.WriteFile(tmp, []byte(state), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path())
}

// HandleTermination orchestrates the alive→syncing→synced transition. Called when
// spec.lifecycle.deletion_requested_at != null.
//
// disker is used to discover rclone units to stop. Returns the new observed state
// (which the caller must POST in /agent/status). When state reaches "synced", the
// caller should also schedule poweroff (see Poweroff method).
//
// Recovery: if sentinel is already "syncing" → continue from there (don't re-stop
// containers that may already be down). If already "synced" → fast-return.
func (m *Manager) HandleTermination(ctx context.Context, disker Disker) (string, error) {
	switch m.CurrentState() {
	case StateSynced, StateDestroyingSelf:
		return StateSynced, nil
	case StateSyncing:
		// Crashed mid-sync → finish the storage unmount pass without re-stopping containers.
		if err := m.flushStorageUnits(ctx, disker); err != nil {
			return StateSyncing, err
		}
		if err := m.SetState(StateSynced); err != nil {
			return StateSynced, fmt.Errorf("persist synced state: %w", err)
		}
		return StateSynced, nil
	default:
		// alive → syncing → synced
		if err := m.SetState(StateSyncing); err != nil {
			return StateAlive, fmt.Errorf("persist syncing state: %w", err)
		}
		if err := m.stopContainers(ctx); err != nil {
			m.log.Warn("docker stop returned error (continuing)", "err", err)
		}
		if err := m.flushStorageUnits(ctx, disker); err != nil {
			return StateSyncing, err
		}
		if err := m.SetState(StateSynced); err != nil {
			return StateSynced, fmt.Errorf("persist synced state: %w", err)
		}
		return StateSynced, nil
	}
}

// Poweroff shuts the VM down. Caller usually delays a few seconds first so the
// final /agent/status POST has a chance to land on backend.
func (m *Manager) Poweroff(ctx context.Context) error {
	if err := m.SetState(StateDestroyingSelf); err != nil {
		m.log.Warn("could not persist destroying_self state", "err", err)
	}
	return m.systemd.Poweroff(ctx)
}

func (m *Manager) stopContainers(ctx context.Context) error {
	// Best-effort: skip silently if docker isn't installed (clean OS strategy).
	if _, err := m.exec.Run(ctx, 5*time.Second, "sh", "-c", "command -v docker"); err != nil {
		return nil
	}
	ids, err := m.exec.Run(ctx, 5*time.Second, "docker", "ps", "-q")
	if err != nil {
		return err
	}
	ids = strings.TrimSpace(ids)
	if ids == "" {
		return nil
	}
	args := []string{"stop", "-t", "30"}
	args = append(args, strings.Fields(ids)...)
	_, err = m.exec.Run(ctx, dockerStopTimeout+10*time.Second, "docker", args...)
	return err
}

func (m *Manager) flushStorageUnits(ctx context.Context, disker Disker) error {
	ids, err := disker.ListUnits()
	if err != nil {
		return fmt.Errorf("list units: %w", err)
	}
	var errs []error
	for _, id := range ids {
		unit := "yougpu-storage-" + id + ".service"
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

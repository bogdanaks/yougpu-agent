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

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/disk"
	"github.com/bogdanaks/yougpu-agent/internal/fetch"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	sentinelFile      = "lifecycle_state"
	storageUnitPrefix = "storage-mount-"

	StateAlive          = "alive"
	StateSyncing        = "syncing"
	StateSynced         = "synced"
	StateDestroyingSelf = "destroying_self"
	StateError          = "error"

	dockerStopTimeout = 30 * time.Second
	dockerKillTimeout = 15 * time.Second
	dockerListTimeout = 5 * time.Second
	defaultStallAfter = 15 * time.Minute
	maxLastError      = 2048
)

type Disker interface {
	ListUnits() ([]string, error)
	Uploads(ctx context.Context, id string) (disk.Uploads, error)
	EnsureRunning(ctx context.Context, id string) error
}

type Hooks struct {
	AfterStop func(ctx context.Context, stopErr error) bool
}

type SystemdStopper interface {
	Stop(ctx context.Context, unit string) error
	Poweroff(ctx context.Context) error
}

type Manager struct {
	stateDir   string
	systemd    SystemdStopper
	exec       system.Executor
	log        *slog.Logger
	stallAfter time.Duration

	mark    *disk.Uploads
	movedAt time.Time
	stall   string
}

func NewManager(stateDir string, systemd SystemdStopper, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{stateDir: stateDir, systemd: systemd, exec: exec, log: log, stallAfter: defaultStallAfter}
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

func (m *Manager) HandleTermination(ctx context.Context, disker Disker, hooks Hooks) (client.StatusLifecycle, error) {
	switch m.CurrentState() {
	case StateSynced, StateDestroyingSelf:
		return client.StatusLifecycle{ObservedState: StateSynced}, nil
	case StateSyncing, StateError:
	default:
		if err := m.SetState(StateSyncing); err != nil {
			return client.StatusLifecycle{ObservedState: StateAlive}, fmt.Errorf("persist syncing state: %w", err)
		}
	}
	stopErr := m.stopContainers(ctx)
	if stopErr != nil {
		m.log.Warn("containers did not stop", "err", stopErr)
	}
	if hooks.AfterStop != nil && !hooks.AfterStop(ctx, stopErr) {
		return client.StatusLifecycle{ObservedState: StateSyncing}, nil
	}
	return m.finishSync(ctx, disker)
}

func (m *Manager) finishSync(ctx context.Context, disker Disker) (client.StatusLifecycle, error) {
	ids, err := disker.ListUnits()
	if err != nil {
		return m.syncing(), fmt.Errorf("list units: %w", err)
	}
	var total disk.Uploads
	known := true
	for _, id := range ids {
		up, err := disker.Uploads(ctx, id)
		if err != nil {
			known = false
			m.log.Warn("disk uploads unknown, making sure rclone runs", "id", id, "err", err)
			if err := disker.EnsureRunning(ctx, id); err != nil {
				m.log.Warn("could not start rclone to finish uploads", "id", id, "err", err)
			}
			continue
		}
		total = sum(total, up)
	}
	if known && total.Pending() == 0 {
		if err := m.flushStorageUnits(ctx, ids); err != nil {
			return m.syncing(), err
		}
		if err := m.SetState(StateSynced); err != nil {
			return client.StatusLifecycle{ObservedState: StateSynced}, fmt.Errorf("persist synced state: %w", err)
		}
		return client.StatusLifecycle{ObservedState: StateSynced}, nil
	}
	if known {
		m.log.Info("waiting for storage uploads before unmount", "pending", total.Pending())
	}
	if msg := m.watch(total, known); msg != "" {
		if err := m.SetState(StateError); err != nil {
			m.log.Warn("could not persist error state", "err", err)
		}
		return client.StatusLifecycle{ObservedState: StateError, LastError: &msg}, nil
	}
	return m.syncing(), nil
}

func (m *Manager) syncing() client.StatusLifecycle {
	if m.stall != "" {
		msg := m.stall
		return client.StatusLifecycle{ObservedState: StateError, LastError: &msg}
	}
	return client.StatusLifecycle{ObservedState: StateSyncing}
}

func (m *Manager) watch(cur disk.Uploads, known bool) string {
	if m.stall != "" {
		return m.stall
	}
	now := time.Now()
	if m.movedAt.IsZero() {
		m.movedAt = now
	}
	if known {
		if m.mark != nil && moved(*m.mark, cur) {
			m.movedAt = now
		}
		m.mark = &cur
	}
	if now.Sub(m.movedAt) < m.stallAfter {
		return ""
	}
	msg := fmt.Sprintf("выгрузка кэша дисков не движется %s: в работе %d, в очереди %d, с ошибками %d", m.stallAfter, cur.InProgress, cur.Queued, cur.Errored)
	if cur.OutOfSpace {
		msg += ", кэшу не хватает места"
	}
	if !known {
		msg += ", rclone не отвечает"
	}
	if cur.LastError != "" {
		msg += "; последняя ошибка rclone: " + cur.LastError
	}
	m.stall = fetch.Clip(msg, maxLastError)
	m.log.Error("storage uploads stalled", "err", m.stall)
	return m.stall
}

func moved(prev, cur disk.Uploads) bool {
	return cur.Completed > prev.Completed || cur.Pending() < prev.Pending() || (cur.Sent > prev.Sent && cur.Errors == prev.Errors)
}

func sum(a, b disk.Uploads) disk.Uploads {
	a.InProgress += b.InProgress
	a.Queued += b.Queued
	a.Errored += b.Errored
	a.OutOfSpace = a.OutOfSpace || b.OutOfSpace
	a.Sent += b.Sent
	a.Completed += b.Completed
	a.Errors += b.Errors
	if b.LastError != "" {
		a.LastError = b.LastError
	}
	return a
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

func (m *Manager) flushStorageUnits(ctx context.Context, ids []string) error {
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

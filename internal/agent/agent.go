// Package agent склеивает все компоненты в один reconcile-loop.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/disk"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
	"github.com/bogdanaks/yougpu-agent/internal/reconcile"
	"github.com/bogdanaks/yougpu-agent/internal/sts"
)

type Config struct {
	Version      string
	PollInterval time.Duration
	Client       *client.Client
	Disk         *disk.Manager
	Lifecycle    *lifecycle.Manager
	STS          *sts.Rotator
	Logger       *slog.Logger
}

type Agent struct {
	cfg     Config
	started time.Time
}

func New(cfg Config) *Agent {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 15 * time.Second
	}
	return &Agent{cfg: cfg, started: time.Now()}
}

// Run blocks until ctx is cancelled (SIGTERM) or the agent finishes its lifecycle
// (synced → poweroff). On synced, schedules a deferred poweroff so the final POST
// /agent/status reaches backend before VM shuts down.
func (a *Agent) Run(ctx context.Context) error {
	a.cfg.Logger.Info("agent run started",
		"version", a.cfg.Version,
		"poll_interval", a.cfg.PollInterval.String(),
	)

	// STS rotation in background (12h ticker; initial cred fetch happens immediately).
	go a.cfg.STS.Run(ctx, a.cfg.Disk)

	// Resume from sentinel: if we already reached "synced" before crash, exit immediately
	// so backend's destroy job can proceed.
	if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
		a.cfg.Logger.Warn("starting in 'synced' state — already finished sync; sleeping until destroy")
		return a.waitForDestroy(ctx)
	}

	t := time.NewTicker(a.cfg.PollInterval)
	defer t.Stop()

	// Run one tick immediately so backend sees us alive before the first interval elapses.
	if err := a.tick(ctx); err != nil {
		a.cfg.Logger.Error("first tick failed", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := a.tick(ctx); err != nil {
				a.cfg.Logger.Error("tick failed", "err", err)
				continue
			}
			if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
				a.cfg.Logger.Info("lifecycle synced — initiating poweroff in 5s")
				time.Sleep(5 * time.Second)
				if err := a.cfg.Lifecycle.Poweroff(ctx); err != nil {
					a.cfg.Logger.Error("poweroff failed", "err", err)
					return err
				}
				return nil
			}
		}
	}
}

func (a *Agent) tick(ctx context.Context) error {
	spec, err := a.cfg.Client.GetSpec(ctx)
	if err != nil {
		return fmt.Errorf("fetch spec: %w", err)
	}

	observedLifecycle := lifecycle.StateAlive
	var disksObserved []client.AgentDiskObserved

	if spec.Lifecycle.DeletionRequestedAt != nil {
		// Skip disk reconciliation while terminating — agent is going down anyway.
		state, err := a.cfg.Lifecycle.HandleTermination(ctx, a.cfg.Disk)
		if err != nil {
			a.cfg.Logger.Error("termination handling failed", "err", err)
		}
		observedLifecycle = state
	} else {
		// Ensure sentinel reflects "alive" (recovery from earlier state).
		_ = a.cfg.Lifecycle.SetState(lifecycle.StateAlive)
		disksObserved = a.reconcileDisks(ctx, spec)
	}

	status := &client.AgentStatus{
		ObservedGeneration: spec.Generation,
		Lifecycle:          client.StatusLifecycle{ObservedState: observedLifecycle},
		Disks:              disksObserved,
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	}
	if err := a.cfg.Client.PostStatus(ctx, status); err != nil {
		return fmt.Errorf("post status: %w", err)
	}
	return nil
}

func (a *Agent) reconcileDisks(ctx context.Context, spec *client.AgentSpec) []client.AgentDiskObserved {
	observed := a.observeDisks(ctx)
	actions := reconcile.Reconcile(spec, observed)

	// Per-disk error map → reported in next status post.
	errs := map[string]string{}
	for _, action := range actions {
		switch v := action.(type) {
		case reconcile.MountDisk:
			a.cfg.Logger.Info("mounting disk", "id", v.Spec.ID, "path", v.Spec.MountPath)
			if err := a.cfg.Disk.Mount(ctx, v.Spec); err != nil {
				a.cfg.Logger.Error("mount failed", "id", v.Spec.ID, "err", err)
				errs[v.Spec.ID] = truncate(err.Error(), 1024)
			}
		case reconcile.UnmountDisk:
			a.cfg.Logger.Info("unmounting disk (desired)", "id", v.ID)
			if err := a.cfg.Disk.Unmount(ctx, v.ID); err != nil {
				a.cfg.Logger.Error("unmount failed", "id", v.ID, "err", err)
				errs[v.ID] = truncate(err.Error(), 1024)
			}
		case reconcile.UnmountOrphan:
			a.cfg.Logger.Info("unmounting orphan unit (no longer in spec)", "id", v.ID)
			if err := a.cfg.Disk.Unmount(ctx, v.ID); err != nil {
				a.cfg.Logger.Error("orphan unmount failed", "id", v.ID, "err", err)
			}
		}
	}

	// Re-observe after applying to report fresh state.
	observed = a.observeDisks(ctx)

	out := make([]client.AgentDiskObserved, 0, len(spec.Disks))
	for _, d := range spec.Disks {
		state := client.ObservedUnmounted
		if observed.MountedDiskIDs[d.ID] {
			state = client.ObservedMounted
		}
		var lastErr *string
		if msg, ok := errs[d.ID]; ok {
			state = client.ObservedError
			lastErr = &msg
		}
		out = append(out, client.AgentDiskObserved{
			ID:            d.ID,
			ObservedState: state,
			LastError:     lastErr,
		})
	}
	return out
}

func (a *Agent) observeDisks(ctx context.Context) reconcile.ObservedState {
	mounted := map[string]bool{}
	unit := map[string]bool{}
	ids, err := a.cfg.Disk.ListUnits()
	if err != nil {
		a.cfg.Logger.Warn("list units failed", "err", err)
		return reconcile.ObservedState{MountedDiskIDs: mounted, UnitDiskIDs: unit}
	}
	for _, id := range ids {
		unit[id] = true
		active, err := a.cfg.Disk.IsActive(ctx, id)
		if err == nil && active {
			mounted[id] = true
		}
	}
	return reconcile.ObservedState{MountedDiskIDs: mounted, UnitDiskIDs: unit}
}

// waitForDestroy — keep posting "synced" until backend kills the VM (or ctx cancels).
// Without this, after crash-recovery in synced state, agent would exit and systemd would
// stop re-running it (Type=simple), so backend's watchdog never sees fresh last_status_at.
func (a *Agent) waitForDestroy(ctx context.Context) error {
	t := time.NewTicker(a.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			status := &client.AgentStatus{
				Lifecycle:    client.StatusLifecycle{ObservedState: lifecycle.StateSynced},
				AgentVersion: a.cfg.Version,
				UptimeSec:    int64(time.Since(a.started).Seconds()),
			}
			// Best-effort — backend may not respond if it's tearing us down.
			if err := a.cfg.Client.PostStatus(ctx, status); err != nil {
				a.cfg.Logger.Warn("post-synced status failed", "err", err)
			}
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

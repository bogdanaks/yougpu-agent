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

func (a *Agent) Run(ctx context.Context) error {
	a.cfg.Logger.Info("agent run started",
		"version", a.cfg.Version,
		"poll_interval", a.cfg.PollInterval.String(),
	)

	go a.cfg.STS.Run(ctx, a.cfg.Disk)

	if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
		a.cfg.Logger.Warn("starting in 'synced' state; waiting for destroy")
		return a.waitForDestroy(ctx)
	}

	t := time.NewTicker(a.cfg.PollInterval)
	defer t.Stop()

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
				a.cfg.Logger.Info("lifecycle synced; initiating poweroff in 5s")
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
		state, err := a.cfg.Lifecycle.HandleTermination(ctx, a.cfg.Disk)
		if err != nil {
			a.cfg.Logger.Error("termination handling failed", "err", err)
		}
		observedLifecycle = state
	} else {
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
			a.cfg.Logger.Info("unmounting disk", "id", v.ID)
			if err := a.cfg.Disk.Unmount(ctx, v.ID); err != nil {
				a.cfg.Logger.Error("unmount failed", "id", v.ID, "err", err)
				errs[v.ID] = truncate(err.Error(), 1024)
			}
		case reconcile.UnmountOrphan:
			a.cfg.Logger.Info("unmounting orphan unit", "id", v.ID)
			if err := a.cfg.Disk.Unmount(ctx, v.ID); err != nil {
				a.cfg.Logger.Error("orphan unmount failed", "id", v.ID, "err", err)
			}
		}
	}

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

// waitForDestroy keeps reporting "synced" until the process is killed externally.
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

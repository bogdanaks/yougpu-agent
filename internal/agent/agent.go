package agent

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/disk"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
	"github.com/bogdanaks/yougpu-agent/internal/reconcile"
	"github.com/bogdanaks/yougpu-agent/internal/sts"
)

type Config struct {
	Version           string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	Client            *client.Client
	Disk              *disk.Manager
	Lifecycle         *lifecycle.Manager
	STS               *sts.Rotator
	Logger            *slog.Logger
}

type Agent struct {
	cfg     Config
	started time.Time
}

func New(cfg Config) *Agent {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	return &Agent{cfg: cfg, started: time.Now()}
}

const (
	sseReconnectMin = 1 * time.Second
	sseReconnectMax = 30 * time.Second
)

func (a *Agent) Run(ctx context.Context) error {
	a.cfg.Logger.Info("agent run started",
		"version", a.cfg.Version,
		"heartbeat_interval", a.cfg.HeartbeatInterval.String(),
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go a.cfg.STS.Run(ctx, a.cfg.Disk)
	go a.heartbeatLoop(ctx, cancel)

	if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
		a.cfg.Logger.Warn("starting in 'synced' state; waiting for destroy")
		return a.waitForDestroy(ctx)
	}

	// SSE-reader → канал спеков. Реconnect с exponential backoff + jitter.
	specs := make(chan *client.AgentSpec, 4)
	go a.streamLoop(ctx, cancel, specs)

	for {
		select {
		case <-ctx.Done():
			return nil
		case spec, ok := <-specs:
			if !ok {
				return nil
			}
			if err := a.handleSpec(ctx, spec); err != nil {
				if client.IsGone(err) {
					a.cfg.Logger.Info("backend returned 410 Gone, stopping agent")
					return nil
				}
				a.cfg.Logger.Error("handle spec failed", "err", err)
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

// streamLoop держит SSE-коннект к backend'у с exponential backoff. На 410/401 — отменяет общий ctx.
func (a *Agent) streamLoop(ctx context.Context, cancel context.CancelFunc, out chan<- *client.AgentSpec) {
	defer close(out)
	backoff := sseReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		err := a.cfg.Client.StreamSpec(ctx, out)
		if ctx.Err() != nil {
			return
		}
		if client.IsGone(err) {
			a.cfg.Logger.Info("SSE got 410, stopping agent")
			cancel()
			return
		}
		if err != nil {
			a.cfg.Logger.Warn("SSE disconnected, reconnecting", "err", err, "backoff", backoff.String())
		}
		// jitter ±20% чтобы N агентов не одновременно ломились на reconnect после backend-restart.
		jitter := time.Duration(rand.Int63n(int64(backoff) / 5))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff + jitter):
		}
		backoff *= 2
		if backoff > sseReconnectMax {
			backoff = sseReconnectMax
		}
	}
}

// heartbeatLoop пингует backend независимо от reconcile. На 410 — cancel'ит общий context.
func (a *Agent) heartbeatLoop(ctx context.Context, cancel context.CancelFunc) {
	t := time.NewTicker(a.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.cfg.Client.Heartbeat(ctx); err != nil {
				if client.IsGone(err) {
					a.cfg.Logger.Info("heartbeat got 410, stopping agent")
					cancel()
					return
				}
				a.cfg.Logger.Warn("heartbeat failed", "err", err)
			}
		}
	}
}

func (a *Agent) handleSpec(ctx context.Context, spec *client.AgentSpec) error {
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

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
	ReconcileInterval time.Duration
	Client            *client.Client
	Disk              *disk.Manager
	Lifecycle         *lifecycle.Manager
	Creds             *sts.Provider
	Logger            *slog.Logger
}

type Agent struct {
	cfg         Config
	started     time.Time
	knownDiskID map[string]bool
	lastSpec    *client.AgentSpec
}

func New(cfg Config) *Agent {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = 60 * time.Second
	}
	return &Agent{
		cfg:         cfg,
		started:     time.Now(),
		knownDiskID: map[string]bool{},
	}
}

const (
	sseReconnectMin = 1 * time.Second
	sseReconnectMax = 30 * time.Second
)

func (a *Agent) Run(ctx context.Context) error {
	a.cfg.Logger.Info("agent run started",
		"version", a.cfg.Version,
		"heartbeat_interval", a.cfg.HeartbeatInterval.String(),
		"reconcile_interval", a.cfg.ReconcileInterval.String(),
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := a.cfg.Creds.EnsureFresh(ctx); err != nil {
		a.cfg.Logger.Error("initial credentials fetch failed", "err", err)
	}

	go a.cfg.Creds.Run(ctx)
	go a.heartbeatLoop(ctx, cancel)

	if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
		a.cfg.Logger.Warn("starting in 'synced' state; waiting for destroy")
		return a.waitForDestroy(ctx)
	}

	specs := make(chan *client.AgentSpec, 4)
	go a.streamLoop(ctx, cancel, specs)

	reconcileTicker := time.NewTicker(a.cfg.ReconcileInterval)
	defer reconcileTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case spec, ok := <-specs:
			if !ok {
				return nil
			}
			a.lastSpec = spec
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
		case <-reconcileTicker.C:
			if a.lastSpec == nil {
				continue
			}
			a.cfg.Logger.Debug("periodic reconcile tick")
			if err := a.handleSpec(ctx, a.lastSpec); err != nil {
				if client.IsGone(err) {
					a.cfg.Logger.Info("backend returned 410 Gone during reconcile, stopping agent")
					return nil
				}
				a.cfg.Logger.Warn("periodic reconcile failed", "err", err)
			}
		}
	}
}

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
		a.refreshCredsIfDiskSetChanged(ctx, spec)
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

func (a *Agent) refreshCredsIfDiskSetChanged(ctx context.Context, spec *client.AgentSpec) {
	specIDs := make(map[string]bool, len(spec.Disks))
	hasNew := false
	for _, d := range spec.Disks {
		specIDs[d.ID] = true
		if !a.knownDiskID[d.ID] {
			hasNew = true
		}
	}
	a.knownDiskID = specIDs
	if !hasNew {
		return
	}
	a.cfg.Logger.Info("disk set changed, refreshing credentials to update scope")
	if err := a.cfg.Creds.ForceRefresh(ctx); err != nil {
		a.cfg.Logger.Error("force refresh on disk-set change failed", "err", err)
	}
}

func (a *Agent) reconcileDisks(ctx context.Context, spec *client.AgentSpec) []client.AgentDiskObserved {
	observed := a.observeDisks(ctx)
	actions := reconcile.Reconcile(spec, observed)

	if len(actions) == 0 {
		a.cfg.Logger.Debug("reconcile no-op",
			"generation", spec.Generation,
			"disks_in_spec", len(spec.Disks),
			"mounted", len(observed.MountedDiskIDs),
		)
	}

	errs := map[string]string{}
	mountErrored := false
	for _, action := range actions {
		switch v := action.(type) {
		case reconcile.MountDisk:
			a.cfg.Logger.Info("mounting disk", "id", v.Spec.ID, "path", v.Spec.MountPath)
			if err := a.cfg.Disk.Mount(ctx, v.Spec); err != nil {
				a.cfg.Logger.Error("mount failed", "id", v.Spec.ID, "err", err)
				errs[v.Spec.ID] = truncate(err.Error(), 1024)
				mountErrored = true
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

	if mountErrored {
		a.cfg.Logger.Warn("mount error detected, forcing credentials refresh and retrying once")
		if err := a.cfg.Creds.ForceRefresh(ctx); err != nil {
			a.cfg.Logger.Error("force refresh after mount error failed", "err", err)
		} else {
			retryActions := reconcile.Reconcile(spec, a.observeDisks(ctx))
			for _, action := range retryActions {
				if v, ok := action.(reconcile.MountDisk); ok {
					a.cfg.Logger.Info("retrying mount after creds refresh", "id", v.Spec.ID)
					if err := a.cfg.Disk.Mount(ctx, v.Spec); err != nil {
						a.cfg.Logger.Error("retry mount failed", "id", v.Spec.ID, "err", err)
						errs[v.Spec.ID] = truncate(err.Error(), 1024)
					} else {
						delete(errs, v.Spec.ID)
					}
				}
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

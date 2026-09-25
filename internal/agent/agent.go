package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/container"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
	"github.com/bogdanaks/yougpu-agent/internal/reconcile"
)

const (
	containerReadyTimeout       = 3 * time.Minute
	containerReadyProbeInterval = 2 * time.Second
	endpointDialTimeout         = 2 * time.Second
	poweroffDelay               = 5 * time.Second
	phaseReportTimeout          = 5 * time.Second
)

type AgentClient interface {
	PostStatus(ctx context.Context, status *client.AgentStatus) error
	StreamSpec(ctx context.Context, out chan<- *client.AgentSpec) error
	Heartbeat(ctx context.Context) error
}

type DiskManager interface {
	Mount(ctx context.Context, spec client.AgentDiskSpec) error
	Unmount(ctx context.Context, id string) error
	ListUnits() ([]string, error)
	IsActive(ctx context.Context, id string) (bool, error)
	PendingUploads(ctx context.Context, id string) (int, error)
}

type ContainerReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentContainerSpec, beforeStart func() bool) client.AgentContainerObserved
	SetReporter(func(context.Context, client.AgentContainerObserved))
}

type FirewallReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentFirewallSpec) client.AgentFirewallObserved
}

type SSHKeysReconciler interface {
	Reconcile(spec *client.AgentSSHSpec) error
}

type TunnelReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentTunnelSpec)
	Ready(subdomains []string) bool
}

type HostSetup interface {
	Reconcile(ctx context.Context) client.AgentSetupObserved
	SetReporter(func(context.Context, client.AgentSetupObserved))
}

type ContentReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentContentSpec, container *client.AgentContainerSpec)
	Observe() (client.AgentContentObserved, bool)
	Stop()
	SetReporter(func(context.Context, client.AgentContentObserved))
	SetNotify(func())
}

type LifecycleManager interface {
	CurrentState() string
	SetState(state string) error
	HandleTermination(ctx context.Context, disker lifecycle.Disker, hooks lifecycle.Hooks) (string, error)
	Poweroff(ctx context.Context) error
}

type StateManager interface {
	Restore(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec) (bool, *client.AgentStateObserved)
	Freeze(ctx context.Context, spec *client.AgentStateSpec, containerName string)
	Save(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec, stopErr error) *client.AgentStateObserved
	Outcome() *client.AgentStateObserved
	SetReporter(func(context.Context, client.AgentStateObserved))
	SetNotify(func())
}

type CredsProvider interface {
	EnsureFresh(ctx context.Context) error
	ForceRefresh(ctx context.Context) error
	Run(ctx context.Context)
}

type Config struct {
	Version           string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	ReconcileInterval time.Duration
	Client            AgentClient
	Disk              DiskManager
	Container         ContainerReconciler
	Firewall          FirewallReconciler
	Tunnel            TunnelReconciler
	HostSetup         HostSetup
	Content           ContentReconciler
	State             StateManager
	SSHKeys           SSHKeysReconciler
	Lifecycle         LifecycleManager
	Creds             CredsProvider
	Logger            *slog.Logger
}

type Agent struct {
	cfg                Config
	started            time.Time
	knownDiskID        map[string]bool
	lastSpec           atomic.Pointer[client.AgentSpec]
	containerReadyHash string
	inbox              inbox
	wake               chan struct{}

	aliveMu     sync.Mutex
	aliveCtx    context.Context
	aliveCancel context.CancelFunc
	deleting    bool
}

type inbox struct {
	mu     sync.Mutex
	latest *client.AgentSpec
	signal chan struct{}
}

func (b *inbox) put(spec *client.AgentSpec) {
	b.mu.Lock()
	b.latest = spec
	b.mu.Unlock()
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *inbox) get() *client.AgentSpec {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.latest
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
	a := &Agent{
		cfg:         cfg,
		started:     time.Now(),
		knownDiskID: map[string]bool{},
		inbox:       inbox{signal: make(chan struct{}, 1)},
		wake:        make(chan struct{}, 1),
	}
	if cfg.Container != nil {
		cfg.Container.SetReporter(a.reportContainerPhase)
	}
	if cfg.HostSetup != nil {
		cfg.HostSetup.SetReporter(a.reportSetupPhase)
	}
	if cfg.Content != nil {
		cfg.Content.SetReporter(a.reportContentPhase)
		cfg.Content.SetNotify(a.poke)
	}
	if cfg.State != nil {
		cfg.State.SetReporter(a.reportStatePhase)
		cfg.State.SetNotify(a.poke)
	}
	return a
}

func (a *Agent) poke() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *Agent) aliveContext(parent context.Context) context.Context {
	a.aliveMu.Lock()
	defer a.aliveMu.Unlock()
	if a.deleting {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx
	}
	if a.aliveCtx == nil || a.aliveCtx.Err() != nil {
		a.aliveCtx, a.aliveCancel = context.WithCancel(parent)
	}
	return a.aliveCtx
}

func (a *Agent) stopAlive() {
	a.aliveMu.Lock()
	defer a.aliveMu.Unlock()
	a.deleting = true
	if a.aliveCancel != nil {
		a.aliveCancel()
	}
}

func (a *Agent) reportPhase(ctx context.Context, kind, state string, fill func(*client.AgentStatus)) {
	spec := a.lastSpec.Load()
	if spec == nil {
		return
	}
	status := &client.AgentStatus{
		ObservedGeneration: spec.Generation,
		Lifecycle:          client.StatusLifecycle{ObservedState: a.cfg.Lifecycle.CurrentState()},
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	}
	fill(status)
	reportCtx, cancel := context.WithTimeout(ctx, phaseReportTimeout)
	defer cancel()
	if err := a.cfg.Client.PostStatus(reportCtx, status); err != nil {
		a.cfg.Logger.Warn(kind+" phase report failed", "state", state, "err", err)
	}
}

func (a *Agent) reportStatePhase(ctx context.Context, obs client.AgentStateObserved) {
	a.reportPhase(ctx, "state", obs.ObservedState, func(s *client.AgentStatus) { s.State = &obs })
}

func (a *Agent) reportContainerPhase(ctx context.Context, obs client.AgentContainerObserved) {
	a.reportPhase(ctx, "container", obs.ObservedState, func(s *client.AgentStatus) { s.Container = &obs })
}

func (a *Agent) reportSetupPhase(ctx context.Context, obs client.AgentSetupObserved) {
	a.reportPhase(ctx, "setup", obs.ObservedState, func(s *client.AgentStatus) { s.Setup = &obs })
}

func (a *Agent) reportContentPhase(ctx context.Context, obs client.AgentContentObserved) {
	a.reportPhase(ctx, "content", obs.ObservedState, func(s *client.AgentStatus) { s.Content = &obs })
}

func (a *Agent) terminationHooks(spec *client.AgentSpec, saved **client.AgentStateObserved) lifecycle.Hooks {
	if a.cfg.State == nil || spec.State == nil || spec.State.Save == nil {
		return lifecycle.Hooks{}
	}
	return lifecycle.Hooks{
		BeforeStop: func(ctx context.Context) {
			a.cfg.State.Freeze(ctx, spec.State, container.AppContainerName)
		},
		AfterStop: func(ctx context.Context, stopErr error) bool {
			obs := a.cfg.State.Save(ctx, spec.State, spec.Container, stopErr)
			*saved = obs
			return obs == nil || obs.ObservedState != client.StateSaving
		},
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
		var httpErr *client.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusBadRequest {
			a.cfg.Logger.Info("no storage drive attached; skipping initial credentials fetch")
		} else {
			a.cfg.Logger.Warn("initial credentials fetch failed; will retry in background", "err", err)
		}
	}

	go a.cfg.Creds.Run(ctx)
	go a.heartbeatLoop(ctx, cancel)

	if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
		a.cfg.Logger.Warn("starting in 'synced' state; waiting for destroy")
		return a.waitForDestroy(ctx)
	}

	specs := make(chan *client.AgentSpec, 4)
	streamDone := make(chan struct{})
	go a.streamLoop(ctx, cancel, specs)
	go a.intake(specs, streamDone)

	reconcileTicker := time.NewTicker(a.cfg.ReconcileInterval)
	defer reconcileTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-streamDone:
			return nil
		case <-a.inbox.signal:
		case <-a.wake:
		case <-reconcileTicker.C:
		}
		spec := a.inbox.get()
		if spec == nil {
			continue
		}
		a.lastSpec.Store(spec)
		if err := a.handleSpec(ctx, spec); err != nil {
			if client.IsGone(err) {
				a.cfg.Logger.Info("backend returned 410 Gone, stopping agent")
				return nil
			}
			a.cfg.Logger.Error("handle spec failed", "err", err)
			continue
		}
		if a.cfg.Lifecycle.CurrentState() == lifecycle.StateSynced {
			return a.powerOff(ctx)
		}
	}
}

func (a *Agent) intake(specs <-chan *client.AgentSpec, done chan<- struct{}) {
	defer close(done)
	for spec := range specs {
		a.inbox.put(spec)
		if spec.Lifecycle.DeletionRequestedAt != nil {
			a.stopAlive()
		}
	}
}

func (a *Agent) powerOff(ctx context.Context) error {
	a.cfg.Logger.Info("lifecycle synced; initiating poweroff", "delay", poweroffDelay.String())
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(poweroffDelay):
	}
	if err := a.cfg.Lifecycle.Poweroff(ctx); err != nil {
		a.cfg.Logger.Error("poweroff failed", "err", err)
		return err
	}
	return nil
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
	if spec.Lifecycle.DeletionRequestedAt != nil {
		return a.terminate(ctx, spec)
	}
	work := a.aliveContext(ctx)
	_ = a.cfg.Lifecycle.SetState(lifecycle.StateAlive)

	if a.cfg.SSHKeys != nil && spec.SSH != nil {
		if err := a.cfg.SSHKeys.Reconcile(spec.SSH); err != nil {
			a.cfg.Logger.Error("ssh keys reconcile failed", "err", err)
		}
	}

	var setupObserved *client.AgentSetupObserved
	if a.cfg.HostSetup != nil {
		obs := a.cfg.HostSetup.Reconcile(work)
		setupObserved = &obs
		if obs.ObservedState != client.SetupReady {
			if work.Err() != nil {
				return nil
			}
			return a.postStatus(ctx, &client.AgentStatus{
				ObservedGeneration: spec.Generation,
				Lifecycle:          client.StatusLifecycle{ObservedState: lifecycle.StateAlive},
				Setup:              setupObserved,
				AgentVersion:       a.cfg.Version,
				UptimeSec:          int64(time.Since(a.started).Seconds()),
			})
		}
	}

	a.refreshCredsIfDiskSetChanged(work, spec)
	disksObserved := a.reconcileDisks(work, spec)

	hasContent := a.cfg.Content != nil && spec.Content != nil
	if hasContent {
		a.cfg.Content.Reconcile(work, spec.Content, spec.Container)
	} else if a.cfg.Content != nil {
		a.cfg.Content.Stop()
	}

	stateReady := true
	var stateObserved *client.AgentStateObserved
	restore := func() {
		if a.cfg.State != nil && spec.State != nil {
			stateReady, stateObserved = a.cfg.State.Restore(work, spec.State, spec.Container)
		}
	}
	restore()

	var containerObserved *client.AgentContainerObserved
	if a.cfg.Container != nil {
		obs := a.cfg.Container.Reconcile(work, spec.Container, func() bool {
			restore()
			if hasContent {
				if _, settled := a.cfg.Content.Observe(); !settled {
					return false
				}
			}
			return stateReady
		})
		containerObserved = &obs
	}
	var firewallObserved *client.AgentFirewallObserved
	if a.cfg.Firewall != nil && spec.Firewall != nil {
		obs := a.cfg.Firewall.Reconcile(work, spec.Firewall)
		firewallObserved = &obs
	}
	if a.cfg.Tunnel != nil {
		a.cfg.Tunnel.Reconcile(ctx, spec.Tunnel)
	}
	if containerObserved != nil && containerObserved.ObservedState == client.ContainerRunning {
		if a.ensureContainerReady(work, spec, containerObserved.SpecHash) {
			containerObserved.ObservedState = client.ContainerReady
		}
	}
	if work.Err() != nil {
		return nil
	}

	var contentObserved *client.AgentContentObserved
	if hasContent {
		obs, _ := a.cfg.Content.Observe()
		contentObserved = &obs
		if obs.ObservedState == client.ContentError {
			a.cfg.Logger.Error("content reconcile failed", "err", deref(obs.LastError))
		}
	}

	return a.postStatus(ctx, &client.AgentStatus{
		ObservedGeneration: spec.Generation,
		Lifecycle:          client.StatusLifecycle{ObservedState: lifecycle.StateAlive},
		Disks:              disksObserved,
		Container:          containerObserved,
		Firewall:           firewallObserved,
		Setup:              setupObserved,
		Content:            contentObserved,
		State:              stateObserved,
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	})
}

func (a *Agent) terminate(ctx context.Context, spec *client.AgentSpec) error {
	a.stopAlive()
	if a.cfg.Content != nil {
		a.cfg.Content.Stop()
	}
	var stateObserved *client.AgentStateObserved
	observed, err := a.cfg.Lifecycle.HandleTermination(ctx, a.cfg.Disk, a.terminationHooks(spec, &stateObserved))
	if err != nil {
		a.cfg.Logger.Error("termination handling failed", "err", err)
	}
	if stateObserved == nil && a.cfg.State != nil {
		stateObserved = a.cfg.State.Outcome()
	}
	return a.postStatus(ctx, &client.AgentStatus{
		ObservedGeneration: spec.Generation,
		Lifecycle:          client.StatusLifecycle{ObservedState: observed},
		State:              stateObserved,
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (a *Agent) ensureContainerReady(ctx context.Context, spec *client.AgentSpec, hash string) bool {
	if spec.Container == nil {
		return true
	}
	ports, ready := a.readinessTargets(spec)
	if len(ports) == 0 {
		return true
	}
	if hash != "" && a.containerReadyHash == hash {
		return true
	}

	deadline := time.Now().Add(containerReadyTimeout)
	for {
		if a.portsListening(ports) && ready() {
			a.containerReadyHash = hash
			a.cfg.Logger.Info("container endpoints reachable; marking ready", "endpoints", len(ports))
			return true
		}
		if time.Now().After(deadline) {
			a.containerReadyHash = hash
			a.cfg.Logger.Warn("container readiness timed out; marking ready (degraded)", "endpoints", len(ports))
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(containerReadyProbeInterval):
		}
	}
}

func (a *Agent) readinessTargets(spec *client.AgentSpec) ([]int, func() bool) {
	if spec.Tunnel != nil && len(spec.Tunnel.Proxies) > 0 {
		ports := make([]int, 0, len(spec.Tunnel.Proxies))
		subdomains := make([]string, 0, len(spec.Tunnel.Proxies))
		for _, p := range spec.Tunnel.Proxies {
			ports = append(ports, p.LocalPort)
			subdomains = append(subdomains, p.Subdomain)
		}
		return ports, func() bool {
			return a.cfg.Tunnel != nil && a.cfg.Tunnel.Ready(subdomains)
		}
	}
	return nil, func() bool { return true }
}

func (a *Agent) portsListening(ports []int) bool {
	for _, port := range ports {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), endpointDialTimeout)
		if err != nil {
			return false
		}
		_ = conn.Close()
	}
	return true
}

func (a *Agent) postStatus(ctx context.Context, status *client.AgentStatus) error {
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
			if a.cfg.State != nil {
				status.State = a.cfg.State.Outcome()
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

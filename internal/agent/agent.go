package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/disk"
	"github.com/bogdanaks/yougpu-agent/internal/fetch"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
	"github.com/bogdanaks/yougpu-agent/internal/reconcile"
)

const (
	containerReadyTimeout       = 3 * time.Minute
	containerReadyProbeInterval = 2 * time.Second
	endpointDialTimeout         = 2 * time.Second
	poweroffDelay               = 5 * time.Second
	phaseReportTimeout          = 5 * time.Second
	maxError                    = 1024
	maxContainerDetail          = 64
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
	MountID(ctx context.Context, id string) (string, error)
	Uploads(ctx context.Context, id string) (disk.Uploads, error)
	EnsureRunning(ctx context.Context, id string) error
}

type ContainerReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentContainerSpec, beforeStart func() bool) client.AgentContainerObserved
	SetReporter(func(context.Context, client.AgentContainerObserved))
	Restart(ctx context.Context) error
}

type FirewallReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentFirewallSpec) client.AgentFirewallObserved
}

type SSHKeysReconciler interface {
	Reconcile(spec *client.AgentSSHSpec) error
}

type TunnelReconciler interface {
	Reconcile(ctx context.Context, spec *client.AgentTunnelSpec)
	Status(subdomains []string) (bool, string)
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
	HandleTermination(ctx context.Context, disker lifecycle.Disker, hooks lifecycle.Hooks) (client.StatusLifecycle, error)
	Poweroff(ctx context.Context) error
}

type StateManager interface {
	Restore(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec) (bool, *client.AgentStateObserved)
	Save(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec, stopErr error) *client.AgentStateObserved
	Checkpoint(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec)
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
	applied            atomic.Int64
	mountIDs           map[string]string
	containerAlive     bool

	postMu       sync.Mutex
	stateReports int
	lastState    *client.AgentStateObserved

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
		ObservedGeneration: a.applied.Load(),
		Lifecycle:          client.StatusLifecycle{ObservedState: a.cfg.Lifecycle.CurrentState()},
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	}
	fill(status)
	reportCtx, cancel := context.WithTimeout(ctx, phaseReportTimeout)
	defer cancel()
	if err := a.postStatus(reportCtx, status, -1); err != nil {
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

	complete := true
	if a.cfg.SSHKeys != nil && spec.SSH != nil {
		if err := a.cfg.SSHKeys.Reconcile(spec.SSH); err != nil {
			complete = false
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
				ObservedGeneration: a.applied.Load(),
				Lifecycle:          client.StatusLifecycle{ObservedState: lifecycle.StateAlive},
				Setup:              setupObserved,
				AgentVersion:       a.cfg.Version,
				UptimeSec:          int64(time.Since(a.started).Seconds()),
			}, -1)
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

	stateSeen := a.stateSeq()
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
		hold := ""
		obs := a.cfg.Container.Reconcile(work, spec.Container, func() bool {
			restore()
			if hasContent {
				if _, settled := a.cfg.Content.Observe(); !settled {
					return false
				}
			}
			if !stateReady {
				return false
			}
			hold = a.diskHold(work, spec)
			return hold == ""
		})
		if hold != "" && obs.ObservedState == client.ContainerPulling {
			obs.ObservedState = client.ContainerStarting
			obs.Detail = &hold
		}
		containerObserved = &obs
		a.followRemounts(work, spec, obs.ObservedState)
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
	if a.cfg.State != nil && spec.State != nil && spec.State.Checkpoint != nil {
		a.cfg.State.Checkpoint(work, spec.State, spec.Container)
	}

	var contentObserved *client.AgentContentObserved
	if hasContent {
		obs, _ := a.cfg.Content.Observe()
		contentObserved = &obs
		if obs.ObservedState == client.ContentError {
			a.cfg.Logger.Error("content reconcile failed", "err", deref(obs.LastError))
		}
	}

	generation := a.applied.Load()
	if complete {
		generation = spec.Generation
	}
	err := a.postStatus(ctx, &client.AgentStatus{
		ObservedGeneration: generation,
		Lifecycle:          client.StatusLifecycle{ObservedState: lifecycle.StateAlive},
		Disks:              disksObserved,
		Container:          containerObserved,
		Firewall:           firewallObserved,
		Setup:              setupObserved,
		Content:            contentObserved,
		State:              stateObserved,
		Tunnel:             a.tunnelObserved(spec),
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	}, stateSeen)
	if err == nil && complete {
		a.applied.Store(spec.Generation)
	}
	return err
}

func (a *Agent) diskHold(ctx context.Context, spec *client.AgentSpec) string {
	mounted := a.observeDisks(ctx).MountedDiskIDs
	for _, d := range spec.Disks {
		if d.DesiredState != client.DesiredMounted || mounted[d.ID] {
			continue
		}
		name := filepath.Base(d.MountPath)
		if d.MountPath == "" || name == "/" || name == "." {
			name = d.ID
		}
		return fetch.Clip("ждём диск "+name, maxContainerDetail)
	}
	return ""
}

func (a *Agent) followRemounts(ctx context.Context, spec *client.AgentSpec, containerState string) {
	current := map[string]string{}
	for _, d := range spec.Disks {
		if d.DesiredState != client.DesiredMounted {
			continue
		}
		id, err := a.cfg.Disk.MountID(ctx, d.ID)
		switch {
		case err == nil && id != "":
			current[d.ID] = id
		case a.mountIDs[d.ID] != "":
			current[d.ID] = a.mountIDs[d.ID]
		}
	}
	alive := containerState == client.ContainerRunning || containerState == client.ContainerReady
	if alive && a.containerAlive {
		for id, now := range current {
			if before, ok := a.mountIDs[id]; ok && before != now {
				a.cfg.Logger.Info("disk remounted under a running container, restarting the container", "id", id)
				if err := a.cfg.Container.Restart(ctx); err != nil {
					a.cfg.Logger.Error("container restart after remount failed", "err", err)
				}
				break
			}
		}
	}
	a.containerAlive = alive
	a.mountIDs = current
}

func (a *Agent) tunnelObserved(spec *client.AgentSpec) *client.AgentTunnelObserved {
	if a.cfg.Tunnel == nil || spec.Tunnel == nil || len(spec.Tunnel.Proxies) == 0 {
		return nil
	}
	subdomains := make([]string, 0, len(spec.Tunnel.Proxies))
	for _, p := range spec.Tunnel.Proxies {
		subdomains = append(subdomains, p.Subdomain)
	}
	if ok, reason := a.cfg.Tunnel.Status(subdomains); !ok {
		msg := fetch.Clip(reason, maxError)
		return &client.AgentTunnelObserved{ObservedState: client.TunnelDisconnected, LastError: &msg}
	}
	return &client.AgentTunnelObserved{ObservedState: client.TunnelConnected}
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
		Lifecycle:          observed,
		State:              stateObserved,
		AgentVersion:       a.cfg.Version,
		UptimeSec:          int64(time.Since(a.started).Seconds()),
	}, -1)
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
			if a.cfg.Tunnel == nil {
				return false
			}
			ok, _ := a.cfg.Tunnel.Status(subdomains)
			return ok
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

func (a *Agent) stateSeq() int {
	a.postMu.Lock()
	defer a.postMu.Unlock()
	return a.stateReports
}

func (a *Agent) postStatus(ctx context.Context, status *client.AgentStatus, stateSeen int) error {
	a.postMu.Lock()
	defer a.postMu.Unlock()
	if stateSeen >= 0 && status.State != nil && a.stateReports != stateSeen && a.lastState != nil {
		status.State = a.lastState
	}
	if err := a.cfg.Client.PostStatus(ctx, status); err != nil {
		return fmt.Errorf("post status: %w", err)
	}
	if status.State != nil {
		a.stateReports++
		a.lastState = status.State
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
				errs[v.Spec.ID] = fetch.Clip(err.Error(), maxError)
				mountErrored = true
			}
		case reconcile.UnmountDisk:
			a.cfg.Logger.Info("unmounting disk", "id", v.ID)
			if err := a.cfg.Disk.Unmount(ctx, v.ID); errors.Is(err, disk.ErrFlushing) {
				a.cfg.Logger.Info("disk keeps uploading its cache, unmount postponed", "id", v.ID)
			} else if err != nil {
				a.cfg.Logger.Error("unmount failed", "id", v.ID, "err", err)
				errs[v.ID] = fetch.Clip(err.Error(), maxError)
			}
		case reconcile.UnmountOrphan:
			a.cfg.Logger.Info("unmounting orphan unit", "id", v.ID)
			if err := a.cfg.Disk.Unmount(ctx, v.ID); errors.Is(err, disk.ErrFlushing) {
				a.cfg.Logger.Info("orphan disk keeps uploading its cache, unmount postponed", "id", v.ID)
			} else if err != nil {
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
						errs[v.Spec.ID] = fetch.Clip(err.Error(), maxError)
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
			if err := a.postStatus(ctx, status, -1); err != nil {
				a.cfg.Logger.Warn("post-synced status failed", "err", err)
			}
		}
	}
}

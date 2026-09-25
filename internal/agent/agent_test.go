package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recorder struct {
	mu    sync.Mutex
	items []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.items = append(r.items, s)
	r.mu.Unlock()
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.items...)
}

func (r *recorder) index(s string) int {
	for i, v := range r.list() {
		if v == s {
			return i
		}
	}
	return -1
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type fakeHostSetup struct {
	rec    *recorder
	obs    client.AgentSetupObserved
	called bool
}

func (f *fakeHostSetup) Reconcile(_ context.Context) client.AgentSetupObserved {
	f.called = true
	f.rec.add("hostsetup")
	return f.obs
}
func (f *fakeHostSetup) SetReporter(func(context.Context, client.AgentSetupObserved)) {}

type fakeSSHKeys struct {
	rec  *recorder
	spec *client.AgentSSHSpec
}

func (f *fakeSSHKeys) Reconcile(spec *client.AgentSSHSpec) error {
	f.spec = spec
	f.rec.add("sshkeys")
	return nil
}

type fakeDisk struct {
	rec        *recorder
	mu         sync.Mutex
	listCalled bool
}

func (f *fakeDisk) Mount(context.Context, client.AgentDiskSpec) error   { return nil }
func (f *fakeDisk) Unmount(context.Context, string) error               { return nil }
func (f *fakeDisk) IsActive(context.Context, string) (bool, error)      { return false, nil }
func (f *fakeDisk) PendingUploads(context.Context, string) (int, error) { return 0, nil }
func (f *fakeDisk) ListUnits() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.listCalled {
		f.rec.add("disk")
	}
	f.listCalled = true
	return nil, nil
}

type fakeContainer struct {
	rec    *recorder
	called bool
}

func (f *fakeContainer) Reconcile(_ context.Context, _ *client.AgentContainerSpec, beforeStart func() bool) client.AgentContainerObserved {
	f.called = true
	f.rec.add("container")
	if beforeStart != nil && !beforeStart() {
		f.rec.add("gate-closed")
		return client.AgentContainerObserved{ObservedState: client.ContainerPulling}
	}
	return client.AgentContainerObserved{ObservedState: client.ContainerRunning}
}
func (f *fakeContainer) SetReporter(func(context.Context, client.AgentContainerObserved)) {}

type fakeFirewall struct {
	rec    *recorder
	called bool
}

func (f *fakeFirewall) Reconcile(context.Context, *client.AgentFirewallSpec) client.AgentFirewallObserved {
	f.called = true
	f.rec.add("firewall")
	return client.AgentFirewallObserved{ObservedState: client.FirewallApplied}
}

type fakeClient struct {
	mu       sync.Mutex
	statuses []*client.AgentStatus
	feed     chan *client.AgentSpec
}

func (f *fakeClient) PostStatus(_ context.Context, s *client.AgentStatus) error {
	f.mu.Lock()
	f.statuses = append(f.statuses, s)
	f.mu.Unlock()
	return nil
}

func (f *fakeClient) StreamSpec(ctx context.Context, out chan<- *client.AgentSpec) error {
	if f.feed == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case spec := <-f.feed:
			select {
			case out <- spec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (f *fakeClient) Heartbeat(context.Context) error { return nil }

func (f *fakeClient) posted() []*client.AgentStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*client.AgentStatus(nil), f.statuses...)
}

func (f *fakeClient) last() *client.AgentStatus {
	posted := f.posted()
	if len(posted) == 0 {
		return nil
	}
	return posted[len(posted)-1]
}

func (f *fakeClient) find(match func(*client.AgentStatus) bool) *client.AgentStatus {
	for _, s := range f.posted() {
		if match(s) {
			return s
		}
	}
	return nil
}

type fakeLifecycle struct {
	mu      sync.Mutex
	current string
	result  string
	calls   int
}

func (f *fakeLifecycle) CurrentState() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current == "" {
		return lifecycle.StateAlive
	}
	return f.current
}
func (f *fakeLifecycle) SetState(string) error          { return nil }
func (f *fakeLifecycle) Poweroff(context.Context) error { return nil }
func (f *fakeLifecycle) HandleTermination(ctx context.Context, _ lifecycle.Disker, hooks lifecycle.Hooks) (string, error) {
	f.mu.Lock()
	f.calls++
	result := f.result
	f.mu.Unlock()
	if result == lifecycle.StateSynced && f.CurrentState() == lifecycle.StateSynced {
		return result, nil
	}
	if hooks.BeforeStop != nil {
		hooks.BeforeStop(ctx)
	}
	if hooks.AfterStop != nil && !hooks.AfterStop(ctx, nil) {
		return lifecycle.StateSyncing, nil
	}
	if result == "" {
		return lifecycle.StateSynced, nil
	}
	return result, nil
}

func (f *fakeLifecycle) terminations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeState struct {
	rec       *recorder
	mu        sync.Mutex
	restoreOK bool
	obs       *client.AgentStateObserved
	saveObs   *client.AgentStateObserved
	outcome   *client.AgentStateObserved
	stopErr   error
	restores  []*client.AgentStateSpec
}

func (f *fakeState) Restore(_ context.Context, spec *client.AgentStateSpec, _ *client.AgentContainerSpec) (bool, *client.AgentStateObserved) {
	f.rec.add("restore")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores = append(f.restores, spec)
	if spec.Pending {
		return false, &client.AgentStateObserved{ObservedState: client.StateWaiting}
	}
	if f.obs != nil {
		return f.restoreOK, f.obs
	}
	return f.restoreOK, &client.AgentStateObserved{ObservedState: client.StateRestoring}
}

func (f *fakeState) Freeze(_ context.Context, _ *client.AgentStateSpec, name string) {
	f.rec.add("freeze " + name)
}

func (f *fakeState) Save(_ context.Context, _ *client.AgentStateSpec, _ *client.AgentContainerSpec, stopErr error) *client.AgentStateObserved {
	f.rec.add("save")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopErr = stopErr
	if f.saveObs != nil {
		return f.saveObs
	}
	return &client.AgentStateObserved{ObservedState: client.StateSaved}
}

func (f *fakeState) Outcome() *client.AgentStateObserved {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.outcome
}

func (f *fakeState) SetReporter(func(context.Context, client.AgentStateObserved)) {}
func (f *fakeState) SetNotify(func())                                             {}

func (f *fakeState) restoreSpecs() []*client.AgentStateSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*client.AgentStateSpec(nil), f.restores...)
}

type fakeContent struct {
	rec     *recorder
	mu      sync.Mutex
	obs     client.AgentContentObserved
	settled bool
	stopped int
	notify  func()
}

func (f *fakeContent) Reconcile(context.Context, *client.AgentContentSpec, *client.AgentContainerSpec) {
	f.rec.add("content")
}

func (f *fakeContent) Observe() (client.AgentContentObserved, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.obs.ObservedState == "" {
		return client.AgentContentObserved{ObservedState: client.ContentDownloading}, f.settled
	}
	return f.obs, f.settled
}

func (f *fakeContent) Stop() {
	f.mu.Lock()
	f.stopped++
	f.mu.Unlock()
}

func (f *fakeContent) SetReporter(func(context.Context, client.AgentContentObserved)) {}

func (f *fakeContent) SetNotify(fn func()) {
	f.mu.Lock()
	f.notify = fn
	f.mu.Unlock()
}

func (f *fakeContent) settle(obs client.AgentContentObserved) {
	f.mu.Lock()
	f.obs = obs
	f.settled = true
	notify := f.notify
	f.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func (f *fakeContent) stops() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

type fakeCreds struct{}

func (fakeCreds) EnsureFresh(context.Context) error  { return nil }
func (fakeCreds) ForceRefresh(context.Context) error { return nil }
func (fakeCreds) Run(context.Context)                {}

func newTestAgent(rec *recorder, setup client.AgentSetupObserved) (*Agent, *fakeClient, *fakeDisk, *fakeContainer, *fakeFirewall, *fakeHostSetup) {
	cl := &fakeClient{}
	disk := &fakeDisk{rec: rec}
	cont := &fakeContainer{rec: rec}
	fw := &fakeFirewall{rec: rec}
	hs := &fakeHostSetup{rec: rec, obs: setup}
	a := New(Config{
		Client:    cl,
		Disk:      disk,
		Container: cont,
		Firewall:  fw,
		HostSetup: hs,
		Lifecycle: &fakeLifecycle{},
		Creds:     fakeCreds{},
		Logger:    testLogger(),
	})
	return a, cl, disk, cont, fw, hs
}

func specWithWork() *client.AgentSpec {
	return &client.AgentSpec{
		Generation: 1,
		Container:  &client.AgentContainerSpec{Image: "img"},
		Firewall:   &client.AgentFirewallSpec{},
	}
}

func deletion(spec *client.AgentSpec) *client.AgentSpec {
	requested := time.Now().Format(time.RFC3339)
	spec.Lifecycle.DeletionRequestedAt = &requested
	return spec
}

func TestHandleSpecGatesDownstreamUntilSetupReady(t *testing.T) {
	rec := &recorder{}
	a, cl, disk, cont, fw, hs := newTestAgent(rec, client.AgentSetupObserved{ObservedState: client.SetupInstallingDocker})
	spec := specWithWork()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if !hs.called {
		t.Error("host-setup must run")
	}
	if disk.listCalled || cont.called || fw.called {
		t.Errorf("downstream must NOT run while setup != ready (disk=%v container=%v firewall=%v)",
			disk.listCalled, cont.called, fw.called)
	}
	posted := cl.posted()
	if len(posted) != 1 || posted[0].Setup == nil {
		t.Fatalf("must post one status carrying setup block, got %d", len(posted))
	}
	if posted[0].Setup.ObservedState != client.SetupInstallingDocker {
		t.Errorf("posted setup state = %s", posted[0].Setup.ObservedState)
	}
}

func TestHandleSpecRunsDownstreamWhenSetupReady(t *testing.T) {
	rec := &recorder{}
	a, cl, disk, cont, fw, _ := newTestAgent(rec, client.AgentSetupObserved{ObservedState: client.SetupReady})
	spec := specWithWork()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if !disk.listCalled || !cont.called || !fw.called {
		t.Errorf("ready host must run downstream (disk=%v container=%v firewall=%v)",
			disk.listCalled, cont.called, fw.called)
	}
	if last := cl.last(); last == nil || last.Setup == nil {
		t.Error("final status must still carry the setup block")
	}
}

func TestHandleSpecOrderHostSetupFirst(t *testing.T) {
	rec := &recorder{}
	a, _, _, _, _, _ := newTestAgent(rec, client.AgentSetupObserved{ObservedState: client.SetupReady})
	spec := specWithWork()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	hs, dk, ct, fw := rec.index("hostsetup"), rec.index("disk"), rec.index("container"), rec.index("firewall")
	if hs < 0 || dk < 0 || ct < 0 || fw < 0 {
		t.Fatalf("all stages must run, order: %v", rec.list())
	}
	if !(hs < dk && dk < ct && ct < fw) {
		t.Errorf("order must be hostsetup→disk→container→firewall, got %v", rec.list())
	}
}

type gatedContainer struct {
	rec     *recorder
	mu      sync.Mutex
	block   chan struct{}
	entered chan struct{}
	calls   int
}

func (g *gatedContainer) Reconcile(ctx context.Context, _ *client.AgentContainerSpec, beforeStart func() bool) client.AgentContainerObserved {
	g.rec.add("pull")
	g.mu.Lock()
	g.calls++
	first := g.calls == 1
	g.mu.Unlock()
	if first && g.entered != nil {
		close(g.entered)
	}
	if first && g.block != nil {
		select {
		case <-g.block:
		case <-ctx.Done():
			g.rec.add("pull-cancelled")
			return client.AgentContainerObserved{ObservedState: client.ContainerError}
		}
	}
	if beforeStart != nil && !beforeStart() {
		g.rec.add("gate-closed")
		return client.AgentContainerObserved{ObservedState: client.ContainerPulling}
	}
	g.rec.add("run")
	return client.AgentContainerObserved{ObservedState: client.ContainerRunning}
}
func (g *gatedContainer) SetReporter(func(context.Context, client.AgentContainerObserved)) {}

func contentAgent(rec *recorder, content *fakeContent, cont ContainerReconciler) (*Agent, *fakeClient) {
	cl := &fakeClient{}
	a := New(Config{
		Client:    cl,
		Disk:      &fakeDisk{rec: rec},
		Container: cont,
		Firewall:  &fakeFirewall{rec: rec},
		HostSetup: &fakeHostSetup{rec: rec, obs: client.AgentSetupObserved{ObservedState: client.SetupReady}},
		Content:   content,
		Lifecycle: &fakeLifecycle{},
		Creds:     fakeCreds{},
		Logger:    testLogger(),
	})
	return a, cl
}

func specWithModel() *client.AgentSpec {
	spec := specWithWork()
	spec.Content = &client.AgentContentSpec{Models: []client.ContentModel{{URL: "http://x/m.bin", Name: "m.bin"}}}
	return spec
}

func TestContainerStartsOnlyAfterContentSettles(t *testing.T) {
	rec := &recorder{}
	content := &fakeContent{rec: rec}
	a, cl := contentAgent(rec, content, &gatedContainer{rec: rec})
	spec := specWithModel()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}
	if rec.index("content") < 0 || rec.index("pull") < 0 || rec.index("gate-closed") < 0 || rec.index("run") >= 0 {
		t.Fatalf("image must be pulled while models download, container not started: %v", rec.list())
	}
	if last := cl.last(); last.Content == nil || last.Content.ObservedState != client.ContentDownloading {
		t.Fatalf("status must carry the download, got %+v", last.Content)
	}

	content.settle(client.AgentContentObserved{ObservedState: client.ContentReady})
	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}
	if rec.index("run") < 0 {
		t.Fatalf("container must start once content settled: %v", rec.list())
	}
	if last := cl.last(); last.Content == nil || last.Content.ObservedState != client.ContentReady {
		t.Fatalf("final status must carry the content block, got %+v", last.Content)
	}
}

func TestHandleSpecStartsContainerWhenModelDownloadFails(t *testing.T) {
	rec := &recorder{}
	failed := "flux.safetensors: http 401"
	content := &fakeContent{rec: rec, obs: client.AgentContentObserved{ObservedState: client.ContentError, LastError: &failed}, settled: true}
	a, cl := contentAgent(rec, content, &gatedContainer{rec: rec})
	spec := specWithModel()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if rec.index("run") < 0 {
		t.Fatalf("container must start even if a model failed, got %v", rec.list())
	}
	last := cl.last()
	if last.Content == nil || last.Content.ObservedState != client.ContentError || last.Content.LastError == nil || *last.Content.LastError != failed {
		t.Fatalf("final status must carry the download error for the backend, got %+v", last.Content)
	}
	if last.Container == nil || last.Container.ObservedState != client.ContainerReady {
		t.Fatalf("container must be reported ready, got %+v", last.Container)
	}
}

func TestContentReportedWithoutContainer(t *testing.T) {
	rec := &recorder{}
	content := &fakeContent{rec: rec, obs: client.AgentContentObserved{ObservedState: client.ContentReady}, settled: true}
	a, cl := contentAgent(rec, content, nil)
	a.cfg.Container = nil
	spec := specWithModel()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if last := cl.last(); last == nil || last.Content == nil {
		t.Fatal("content result must be reported even without a container")
	}
}

func TestSpecWithoutContentStopsDownloads(t *testing.T) {
	rec := &recorder{}
	content := &fakeContent{rec: rec}
	a, cl := contentAgent(rec, content, &gatedContainer{rec: rec})
	spec := specWithWork()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if content.stops() != 1 || rec.index("content") >= 0 {
		t.Fatalf("stale downloads must stop, stops=%d events=%v", content.stops(), rec.list())
	}
	if last := cl.last(); last.Content != nil || rec.index("run") < 0 {
		t.Fatalf("no content gate without content, got %+v %v", last.Content, rec.list())
	}
}

func TestHandleSpecAppliesSSHKeysBeforeHostIsReady(t *testing.T) {
	rec := &recorder{}
	a, _, _, _, _, _ := newTestAgent(rec, client.AgentSetupObserved{ObservedState: client.SetupInstallingDocker})
	keys := &fakeSSHKeys{rec: rec}
	a.cfg.SSHKeys = keys
	spec := specWithWork()
	spec.SSH = &client.AgentSSHSpec{User: "ubuntu", AuthorizedKeys: []string{"ssh-ed25519 AAAA me@mac"}}
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if keys.spec != spec.SSH {
		t.Fatal("ssh keys from spec must be applied")
	}
	if rec.index("sshkeys") > rec.index("hostsetup") {
		t.Errorf("ssh keys must not wait for host setup, order = %v", rec.list())
	}
}

func stateAgent(rec *recorder, st *fakeState) (*Agent, *fakeClient) {
	cl := &fakeClient{}
	a := New(Config{
		Client:    cl,
		Disk:      &fakeDisk{rec: rec},
		Container: &fakeContainer{rec: rec},
		Firewall:  &fakeFirewall{rec: rec},
		HostSetup: &fakeHostSetup{rec: rec, obs: client.AgentSetupObserved{ObservedState: client.SetupReady}},
		State:     st,
		Lifecycle: &fakeLifecycle{},
		Creds:     fakeCreds{},
		Logger:    testLogger(),
	})
	return a, cl
}

func TestContainerWaitsForWorkspaceState(t *testing.T) {
	rec := &recorder{}
	a, cl := stateAgent(rec, &fakeState{rec: rec})
	spec := specWithWork()
	spec.State = &client.AgentStateSpec{Pending: true}
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if rec.index("gate-closed") < 0 {
		t.Fatalf("container must not start before the state is restored, got %v", rec.list())
	}
	last := cl.last()
	if last.State == nil || last.State.ObservedState != client.StateWaiting {
		t.Fatalf("final status must carry the state, got %+v", last.State)
	}
	if last.Container == nil || last.Container.ObservedState != client.ContainerPulling {
		t.Fatalf("container must stay pulling, got %+v", last.Container)
	}
}

func TestSpecWithoutStateSkipsRestore(t *testing.T) {
	rec := &recorder{}
	a, _ := stateAgent(rec, &fakeState{rec: rec})
	spec := specWithWork()
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}
	if rec.index("restore") >= 0 || rec.index("gate-closed") >= 0 {
		t.Fatalf("state must not gate a spec without it, got %v", rec.list())
	}
}

func TestTerminationFreezesThenSavesState(t *testing.T) {
	rec := &recorder{}
	a, cl := stateAgent(rec, &fakeState{rec: rec})
	spec := deletion(specWithWork())
	spec.State = &client.AgentStateSpec{Save: &client.StateSave{UploadURL: "http://b2/put"}}
	a.lastSpec.Store(spec)

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	freeze, save := rec.index("freeze app_container"), rec.index("save")
	if freeze < 0 || save < 0 || freeze > save {
		t.Fatalf("want freeze then save, got %v", rec.list())
	}
	last := cl.last()
	if last.State == nil || last.State.ObservedState != client.StateSaved {
		t.Fatalf("final status must carry the saved state, got %+v", last.State)
	}
}

func TestSaveInProgressKeepsTerminationSyncing(t *testing.T) {
	rec := &recorder{}
	msg := "state upload: http 503"
	st := &fakeState{rec: rec, saveObs: &client.AgentStateObserved{ObservedState: client.StateSaving, LastError: &msg}}
	a, _ := stateAgent(rec, st)
	spec := deletion(specWithWork())
	spec.State = &client.AgentStateSpec{Save: &client.StateSave{UploadURL: "http://b2/put"}}
	var saved *client.AgentStateObserved
	hooks := a.terminationHooks(spec, &saved)
	alive := errors.New("контейнер не остановился: abc")

	if hooks.AfterStop(context.Background(), alive) {
		t.Fatal("termination must wait while the save is being retried")
	}
	if st.stopErr != alive || saved == nil || saved.ObservedState != client.StateSaving {
		t.Fatalf("stop error must reach the save: %v %+v", st.stopErr, saved)
	}
	for _, state := range []string{client.StateSaved, client.StateSaveFailed, client.StateSaveSkipped} {
		st.saveObs = &client.AgentStateObserved{ObservedState: state}
		if !hooks.AfterStop(context.Background(), nil) {
			t.Fatalf("%s must let termination continue", state)
		}
	}
}

func TestSavedStateRepeatedAfterSync(t *testing.T) {
	rec := &recorder{}
	sum := "abc"
	st := &fakeState{rec: rec, outcome: &client.AgentStateObserved{ObservedState: client.StateSaved, SHA256: &sum}}
	a, cl := stateAgent(rec, st)
	a.cfg.Lifecycle = &fakeLifecycle{current: lifecycle.StateSynced, result: lifecycle.StateSynced}
	spec := deletion(specWithWork())
	spec.State = &client.AgentStateSpec{Save: &client.StateSave{UploadURL: "http://b2/put"}}

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	last := cl.last()
	if last.Lifecycle.ObservedState != lifecycle.StateSynced || last.State == nil || last.State.ObservedState != client.StateSaved {
		t.Fatalf("synced status must repeat the saved state, got %+v", last)
	}
	if rec.index("save") >= 0 {
		t.Fatalf("synced agent must not pack again: %v", rec.list())
	}
}

func TestWaitForDestroyRepeatsSavedState(t *testing.T) {
	rec := &recorder{}
	st := &fakeState{rec: rec, outcome: &client.AgentStateObserved{ObservedState: client.StateSaveSkipped}}
	a, cl := stateAgent(rec, st)
	a.cfg.Lifecycle = &fakeLifecycle{current: lifecycle.StateSynced}
	a.cfg.PollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error)
	go func() { done <- a.Run(ctx) }()

	eventually(t, "a synced status", func() bool { return cl.last() != nil })
	cancel()
	<-done

	last := cl.last()
	if last.Lifecycle.ObservedState != lifecycle.StateSynced || last.State == nil || last.State.ObservedState != client.StateSaveSkipped {
		t.Fatalf("got %+v", last)
	}
}

func TestPhaseReportsCarryPersistedLifecycle(t *testing.T) {
	rec := &recorder{}
	a, cl := stateAgent(rec, &fakeState{rec: rec})
	a.cfg.Lifecycle = &fakeLifecycle{current: lifecycle.StateSyncing}
	a.lastSpec.Store(specWithWork())

	a.reportContentPhase(context.Background(), client.AgentContentObserved{ObservedState: client.ContentDownloading})
	a.reportContainerPhase(context.Background(), client.AgentContainerObserved{ObservedState: client.ContainerPulling})

	for _, s := range cl.posted() {
		if s.Lifecycle.ObservedState != lifecycle.StateSyncing {
			t.Fatalf("phase report must not claim alive during termination: %+v", s.Lifecycle)
		}
	}
}

type runHarness struct {
	agent   *Agent
	client  *fakeClient
	cancel  context.CancelFunc
	done    chan error
	content *fakeContent
	state   *fakeState
	life    *fakeLifecycle
}

func startRun(t *testing.T, rec *recorder, cont ContainerReconciler) *runHarness {
	t.Helper()
	cl := &fakeClient{feed: make(chan *client.AgentSpec)}
	content := &fakeContent{rec: rec}
	st := &fakeState{rec: rec}
	life := &fakeLifecycle{result: lifecycle.StateSyncing}
	a := New(Config{
		Client:            cl,
		Disk:              &fakeDisk{rec: rec},
		Container:         cont,
		Firewall:          &fakeFirewall{rec: rec},
		HostSetup:         &fakeHostSetup{rec: rec, obs: client.AgentSetupObserved{ObservedState: client.SetupReady}},
		Content:           content,
		State:             st,
		Lifecycle:         life,
		Creds:             fakeCreds{},
		Logger:            testLogger(),
		ReconcileInterval: time.Hour,
		HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h := &runHarness{agent: a, client: cl, cancel: cancel, done: make(chan error, 1), content: content, state: st, life: life}
	go func() { h.done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-h.done
	})
	return h
}

func (h *runHarness) send(spec *client.AgentSpec) {
	h.client.feed <- spec
}

func TestDeletionSpecInterruptsAliveWork(t *testing.T) {
	rec := &recorder{}
	cont := &gatedContainer{rec: rec, block: make(chan struct{}), entered: make(chan struct{})}
	h := startRun(t, rec, cont)

	h.send(specWithModel())
	<-cont.entered
	h.send(deletion(&client.AgentSpec{Generation: 2}))

	eventually(t, "termination status", func() bool {
		return h.client.find(func(s *client.AgentStatus) bool { return s.ObservedGeneration == 2 }) != nil
	})
	if rec.index("pull-cancelled") < 0 {
		t.Fatalf("alive work must be cancelled by the deletion spec: %v", rec.list())
	}
	if h.content.stops() == 0 {
		t.Fatal("model downloads must be stopped on deletion")
	}
	if h.life.terminations() == 0 {
		t.Fatal("termination not handled")
	}
	if s := h.client.find(func(s *client.AgentStatus) bool { return s.ObservedGeneration == 1 }); s != nil {
		t.Fatalf("interrupted alive pass must not post a status: %+v", s)
	}
}

func TestLatestSpecWinsAfterLongPass(t *testing.T) {
	rec := &recorder{}
	cont := &gatedContainer{rec: rec, block: make(chan struct{}), entered: make(chan struct{})}
	h := startRun(t, rec, cont)

	h.send(specWithModel())
	<-cont.entered
	for gen := int64(2); gen <= 4; gen++ {
		spec := specWithModel()
		spec.Generation = gen
		h.send(spec)
	}
	eventually(t, "latest spec in the inbox", func() bool {
		latest := h.agent.inbox.get()
		return latest != nil && latest.Generation == 4
	})
	close(cont.block)

	eventually(t, "status for the latest spec", func() bool {
		return h.client.find(func(s *client.AgentStatus) bool { return s.ObservedGeneration == 4 }) != nil
	})
	for _, s := range h.client.posted() {
		if s.ObservedGeneration == 2 || s.ObservedGeneration == 3 {
			t.Fatalf("stale spec %d was processed", s.ObservedGeneration)
		}
	}
}

func TestRestoreStartsWhileModelsStillDownload(t *testing.T) {
	rec := &recorder{}
	h := startRun(t, rec, &gatedContainer{rec: rec})

	spec := specWithModel()
	spec.State = &client.AgentStateSpec{Pending: true}
	h.send(spec)
	eventually(t, "waiting status", func() bool {
		return h.client.find(func(s *client.AgentStatus) bool {
			return s.State != nil && s.State.ObservedState == client.StateWaiting
		}) != nil
	})

	next := specWithModel()
	next.Generation = 2
	next.State = &client.AgentStateSpec{Restore: &client.StateRestore{URL: "http://b2/get", SHA256: "abc", SizeBytes: 1}}
	h.send(next)

	eventually(t, "restore of the saved state", func() bool {
		for _, s := range h.state.restoreSpecs() {
			if !s.Pending {
				return true
			}
		}
		return false
	})
	if _, settled := h.content.Observe(); settled {
		t.Fatal("models were meant to be still downloading")
	}
}

func TestSettledContentWakesTheAgent(t *testing.T) {
	rec := &recorder{}
	h := startRun(t, rec, &gatedContainer{rec: rec})

	h.send(specWithModel())
	eventually(t, "gate closed while models download", func() bool { return rec.index("gate-closed") >= 0 })

	h.content.settle(client.AgentContentObserved{ObservedState: client.ContentReady})

	eventually(t, "container start after content settled", func() bool { return rec.index("run") >= 0 })
}

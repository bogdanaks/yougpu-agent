package agent

import (
	"context"
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

type fakeDisk struct {
	rec        *recorder
	listCalled bool
}

func (f *fakeDisk) Mount(context.Context, client.AgentDiskSpec) error   { return nil }
func (f *fakeDisk) Unmount(context.Context, string) error               { return nil }
func (f *fakeDisk) IsActive(context.Context, string) (bool, error)      { return false, nil }
func (f *fakeDisk) PendingUploads(context.Context, string) (int, error) { return 0, nil }
func (f *fakeDisk) ListUnits() ([]string, error) {
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

func (f *fakeContainer) Reconcile(_ context.Context, _ *client.AgentContainerSpec, beforeStart func()) client.AgentContainerObserved {
	f.called = true
	f.rec.add("container")
	if beforeStart != nil {
		beforeStart()
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
}

func (f *fakeClient) PostStatus(_ context.Context, s *client.AgentStatus) error {
	f.mu.Lock()
	f.statuses = append(f.statuses, s)
	f.mu.Unlock()
	return nil
}
func (f *fakeClient) StreamSpec(context.Context, chan<- *client.AgentSpec) error { return nil }
func (f *fakeClient) Heartbeat(context.Context) error                            { return nil }

func (f *fakeClient) posted() []*client.AgentStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*client.AgentStatus(nil), f.statuses...)
}

type fakeLifecycle struct{}

func (fakeLifecycle) CurrentState() string           { return lifecycle.StateAlive }
func (fakeLifecycle) SetState(string) error          { return nil }
func (fakeLifecycle) Poweroff(context.Context) error { return nil }
func (fakeLifecycle) HandleTermination(context.Context, lifecycle.Disker) (string, error) {
	return lifecycle.StateSynced, nil
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
		Lifecycle: fakeLifecycle{},
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

func TestHandleSpecGatesDownstreamUntilSetupReady(t *testing.T) {
	rec := &recorder{}
	a, cl, disk, cont, fw, hs := newTestAgent(rec, client.AgentSetupObserved{ObservedState: client.SetupInstallingDocker})
	spec := specWithWork()
	a.lastSpec = spec

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
	a.lastSpec = spec

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if !disk.listCalled || !cont.called || !fw.called {
		t.Errorf("ready host must run downstream (disk=%v container=%v firewall=%v)",
			disk.listCalled, cont.called, fw.called)
	}
	posted := cl.posted()
	if len(posted) == 0 || posted[len(posted)-1].Setup == nil {
		t.Error("final status must still carry the setup block")
	}
}

func TestHandleSpecOrderHostSetupFirst(t *testing.T) {
	rec := &recorder{}
	a, _, _, _, _, _ := newTestAgent(rec, client.AgentSetupObserved{ObservedState: client.SetupReady})
	spec := specWithWork()
	a.lastSpec = spec

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
	rec      *recorder
	pullSeen chan struct{}
}

func (g *gatedContainer) Reconcile(_ context.Context, _ *client.AgentContainerSpec, beforeStart func()) client.AgentContainerObserved {
	g.rec.add("pull")
	close(g.pullSeen)
	if beforeStart != nil {
		beforeStart()
	}
	g.rec.add("run")
	return client.AgentContainerObserved{ObservedState: client.ContainerRunning}
}
func (g *gatedContainer) SetReporter(func(context.Context, client.AgentContainerObserved)) {}

type fakeContent struct {
	rec      *recorder
	pullSeen chan struct{}
	obs      client.AgentContentObserved
}

func (f *fakeContent) Reconcile(context.Context, *client.AgentContentSpec, *client.AgentContainerSpec) client.AgentContentObserved {
	select {
	case <-f.pullSeen:
		f.rec.add("content")
	case <-time.After(2 * time.Second):
		f.rec.add("content_without_pull")
	}
	return f.obs
}
func (f *fakeContent) SetReporter(func(context.Context, client.AgentContentObserved)) {}

func TestHandleSpecPullsWhileContentDownloads(t *testing.T) {
	rec := &recorder{}
	pullSeen := make(chan struct{})
	cl := &fakeClient{}
	a := New(Config{
		Client:    cl,
		Disk:      &fakeDisk{rec: rec},
		Container: &gatedContainer{rec: rec, pullSeen: pullSeen},
		Firewall:  &fakeFirewall{rec: rec},
		HostSetup: &fakeHostSetup{rec: rec, obs: client.AgentSetupObserved{ObservedState: client.SetupReady}},
		Content:   &fakeContent{rec: rec, pullSeen: pullSeen, obs: client.AgentContentObserved{ObservedState: client.ContentReady}},
		Lifecycle: fakeLifecycle{},
		Creds:     fakeCreds{},
		Logger:    testLogger(),
	})
	spec := specWithWork()
	spec.Content = &client.AgentContentSpec{Models: []client.ContentModel{{URL: "http://x/m.bin", Name: "m.bin"}}}
	a.lastSpec = spec

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	pull, content, run := rec.index("pull"), rec.index("content"), rec.index("run")
	if pull < 0 || content < 0 || run < 0 {
		t.Fatalf("expected pull/content/run, got %v", rec.list())
	}
	if pull > content {
		t.Errorf("pull must start before content finishes, got %v", rec.list())
	}
	if content > run {
		t.Errorf("container must start only after content is done, got %v", rec.list())
	}

	posted := cl.posted()
	if len(posted) == 0 || posted[len(posted)-1].Content == nil {
		t.Fatalf("final status must carry the content block, got %d statuses", len(posted))
	}
}

func TestHandleSpecStartsContainerWhenModelDownloadFails(t *testing.T) {
	rec := &recorder{}
	pullSeen := make(chan struct{})
	cl := &fakeClient{}
	failed := "flux.safetensors: http 401"
	a := New(Config{
		Client:    cl,
		Disk:      &fakeDisk{rec: rec},
		Container: &gatedContainer{rec: rec, pullSeen: pullSeen},
		Firewall:  &fakeFirewall{rec: rec},
		HostSetup: &fakeHostSetup{rec: rec, obs: client.AgentSetupObserved{ObservedState: client.SetupReady}},
		Content:   &fakeContent{rec: rec, pullSeen: pullSeen, obs: client.AgentContentObserved{ObservedState: client.ContentError, LastError: &failed}},
		Lifecycle: fakeLifecycle{},
		Creds:     fakeCreds{},
		Logger:    testLogger(),
	})
	spec := specWithWork()
	spec.Content = &client.AgentContentSpec{Models: []client.ContentModel{{URL: "http://x/flux.safetensors", Name: "flux.safetensors"}}}
	a.lastSpec = spec

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if rec.index("run") < 0 {
		t.Fatalf("container must start even if a model failed, got %v", rec.list())
	}
	posted := cl.posted()
	last := posted[len(posted)-1]
	if last.Content == nil || last.Content.ObservedState != client.ContentError || last.Content.LastError == nil || *last.Content.LastError != failed {
		t.Fatalf("final status must carry the download error for the backend, got %+v", last.Content)
	}
	if last.Container == nil || last.Container.ObservedState != client.ContainerReady {
		t.Fatalf("container must be reported ready, got %+v", last.Container)
	}
}

func TestHandleSpecWaitsForContentWhenContainerUnchanged(t *testing.T) {
	rec := &recorder{}
	pullSeen := make(chan struct{})
	close(pullSeen)
	cl := &fakeClient{}
	a := New(Config{
		Client:    cl,
		Disk:      &fakeDisk{rec: rec},
		Firewall:  &fakeFirewall{rec: rec},
		HostSetup: &fakeHostSetup{rec: rec, obs: client.AgentSetupObserved{ObservedState: client.SetupReady}},
		Content:   &fakeContent{rec: rec, pullSeen: pullSeen, obs: client.AgentContentObserved{ObservedState: client.ContentReady}},
		Lifecycle: fakeLifecycle{},
		Creds:     fakeCreds{},
		Logger:    testLogger(),
	})
	spec := specWithWork()
	spec.Content = &client.AgentContentSpec{Models: []client.ContentModel{{URL: "http://x/m.bin", Name: "m.bin"}}}
	a.lastSpec = spec

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	posted := cl.posted()
	if len(posted) == 0 || posted[len(posted)-1].Content == nil {
		t.Fatalf("content result must be reported even without a container, got %d statuses", len(posted))
	}
}

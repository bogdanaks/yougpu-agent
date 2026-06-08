package agent

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeHostSetup struct {
	rec    *[]string
	obs    client.AgentSetupObserved
	called bool
}

func (f *fakeHostSetup) Reconcile(_ context.Context) client.AgentSetupObserved {
	f.called = true
	*f.rec = append(*f.rec, "hostsetup")
	return f.obs
}
func (f *fakeHostSetup) SetReporter(func(context.Context, client.AgentSetupObserved)) {}

type fakeDisk struct {
	rec        *[]string
	listCalled bool
}

func (f *fakeDisk) Mount(context.Context, client.AgentDiskSpec) error { return nil }
func (f *fakeDisk) Unmount(context.Context, string) error             { return nil }
func (f *fakeDisk) IsActive(context.Context, string) (bool, error)    { return false, nil }
func (f *fakeDisk) ListUnits() ([]string, error) {
	if !f.listCalled {
		*f.rec = append(*f.rec, "disk")
	}
	f.listCalled = true
	return nil, nil
}

type fakeContainer struct {
	rec    *[]string
	called bool
}

func (f *fakeContainer) Reconcile(context.Context, *client.AgentContainerSpec) client.AgentContainerObserved {
	f.called = true
	*f.rec = append(*f.rec, "container")
	return client.AgentContainerObserved{ObservedState: client.ContainerRunning}
}
func (f *fakeContainer) SetReporter(func(context.Context, client.AgentContainerObserved)) {}

type fakeFirewall struct {
	rec    *[]string
	called bool
}

func (f *fakeFirewall) Reconcile(context.Context, *client.AgentFirewallSpec) client.AgentFirewallObserved {
	f.called = true
	*f.rec = append(*f.rec, "firewall")
	return client.AgentFirewallObserved{ObservedState: client.FirewallApplied}
}

type fakeClient struct {
	statuses []*client.AgentStatus
}

func (f *fakeClient) PostStatus(_ context.Context, s *client.AgentStatus) error {
	f.statuses = append(f.statuses, s)
	return nil
}
func (f *fakeClient) StreamSpec(context.Context, chan<- *client.AgentSpec) error { return nil }
func (f *fakeClient) Heartbeat(context.Context) error                            { return nil }

type fakeLifecycle struct{}

func (fakeLifecycle) CurrentState() string      { return lifecycle.StateAlive }
func (fakeLifecycle) SetState(string) error     { return nil }
func (fakeLifecycle) Poweroff(context.Context) error { return nil }
func (fakeLifecycle) HandleTermination(context.Context, lifecycle.Disker) (string, error) {
	return lifecycle.StateSynced, nil
}

type fakeCreds struct{}

func (fakeCreds) EnsureFresh(context.Context) error  { return nil }
func (fakeCreds) ForceRefresh(context.Context) error { return nil }
func (fakeCreds) Run(context.Context)                {}

func newTestAgent(rec *[]string, setup client.AgentSetupObserved) (*Agent, *fakeClient, *fakeDisk, *fakeContainer, *fakeFirewall, *fakeHostSetup) {
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
	var order []string
	a, cl, disk, cont, fw, hs := newTestAgent(&order, client.AgentSetupObserved{ObservedState: client.SetupInstallingDocker})
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
	if len(cl.statuses) != 1 || cl.statuses[0].Setup == nil {
		t.Fatalf("must post one status carrying setup block, got %d", len(cl.statuses))
	}
	if cl.statuses[0].Setup.ObservedState != client.SetupInstallingDocker {
		t.Errorf("posted setup state = %s", cl.statuses[0].Setup.ObservedState)
	}
}

func TestHandleSpecRunsDownstreamWhenSetupReady(t *testing.T) {
	var order []string
	a, cl, disk, cont, fw, _ := newTestAgent(&order, client.AgentSetupObserved{ObservedState: client.SetupReady})
	spec := specWithWork()
	a.lastSpec = spec

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	if !disk.listCalled || !cont.called || !fw.called {
		t.Errorf("ready host must run downstream (disk=%v container=%v firewall=%v)",
			disk.listCalled, cont.called, fw.called)
	}
	if len(cl.statuses) == 0 || cl.statuses[len(cl.statuses)-1].Setup == nil {
		t.Error("final status must still carry the setup block")
	}
}

func TestHandleSpecOrderHostSetupFirst(t *testing.T) {
	var order []string
	a, _, _, _, _, _ := newTestAgent(&order, client.AgentSetupObserved{ObservedState: client.SetupReady})
	spec := specWithWork()
	a.lastSpec = spec

	if err := a.handleSpec(context.Background(), spec); err != nil {
		t.Fatalf("handleSpec: %v", err)
	}

	idx := func(name string) int {
		for i, v := range order {
			if v == name {
				return i
			}
		}
		return -1
	}
	hs, dk, ct, fw := idx("hostsetup"), idx("disk"), idx("container"), idx("firewall")
	if hs < 0 || dk < 0 || ct < 0 || fw < 0 {
		t.Fatalf("all stages must run, order: %v", order)
	}
	if !(hs < dk && dk < ct && ct < fw) {
		t.Errorf("order must be hostsetup→disk→container→firewall, got %v", order)
	}
}

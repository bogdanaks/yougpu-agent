package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/disk"
)

type fakeDisker struct {
	units   []string
	pending map[string]int
	uploads map[string]disk.Uploads
	err     error
	started []string
}

func (f *fakeDisker) ListUnits() ([]string, error) { return f.units, nil }
func (f *fakeDisker) Uploads(_ context.Context, id string) (disk.Uploads, error) {
	if f.err != nil {
		return disk.Uploads{}, f.err
	}
	if up, ok := f.uploads[id]; ok {
		return up, nil
	}
	return disk.Uploads{Queued: f.pending[id]}, nil
}
func (f *fakeDisker) EnsureRunning(_ context.Context, id string) error {
	f.started = append(f.started, id)
	return nil
}

type fakeStopper struct{ stopped []string }

func (f *fakeStopper) Stop(_ context.Context, unit string) error {
	f.stopped = append(f.stopped, unit)
	return nil
}
func (f *fakeStopper) Poweroff(context.Context) error { return nil }

type noDocker struct{}

func (noDocker) Run(context.Context, time.Duration, string, ...string) (string, error) {
	return "", errors.New("docker not installed")
}

func newTestManager(t *testing.T) (*Manager, *fakeStopper) {
	t.Helper()
	stopper := &fakeStopper{}
	return NewManager(t.TempDir(), stopper, noDocker{}, slog.New(slog.NewTextHandler(io.Discard, nil))), stopper
}

func TestTerminationWaitsForUploadsBeforeStoppingMounts(t *testing.T) {
	m, stopper := newTestManager(t)
	disks := &fakeDisker{units: []string{"d1"}, pending: map[string]int{"d1": 2}}

	state, err := m.HandleTermination(context.Background(), disks, Hooks{})
	if err != nil || state.ObservedState != StateSyncing {
		t.Fatalf("state = %+v, %v; want syncing", state, err)
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped with uploads pending: %v", stopper.stopped)
	}

	disks.pending["d1"] = 0
	state, err = m.HandleTermination(context.Background(), disks, Hooks{})
	if err != nil || state.ObservedState != StateSynced {
		t.Fatalf("state = %+v, %v; want synced", state, err)
	}
	if want := []string{"storage-mount-d1.service"}; !reflect.DeepEqual(stopper.stopped, want) {
		t.Fatalf("stopped = %v; want %v", stopper.stopped, want)
	}
}

func TestTerminationStopsMountsRightAwayWhenNothingPending(t *testing.T) {
	m, stopper := newTestManager(t)

	state, err := m.HandleTermination(context.Background(), &fakeDisker{units: []string{"d1", "d2"}}, Hooks{})
	if err != nil || state.ObservedState != StateSynced {
		t.Fatalf("state = %+v, %v; want synced", state, err)
	}
	if want := []string{"storage-mount-d1.service", "storage-mount-d2.service"}; !reflect.DeepEqual(stopper.stopped, want) {
		t.Fatalf("stopped = %v; want %v", stopper.stopped, want)
	}
}

func TestTerminationKeepsSyncingWhenUploadsUnknown(t *testing.T) {
	m, stopper := newTestManager(t)
	disks := &fakeDisker{units: []string{"d1"}, err: errors.New("rc timeout")}

	state, _ := m.HandleTermination(context.Background(), disks, Hooks{})
	if state.ObservedState != StateSyncing {
		t.Fatalf("state = %+v; want syncing", state)
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped while uploads unknown: %v", stopper.stopped)
	}
	if !reflect.DeepEqual(disks.started, []string{"d1"}) {
		t.Fatalf("rclone of a disk with unknown uploads must be brought back to finish them, started %v", disks.started)
	}
}

func stalling(m *Manager) {
	m.stallAfter = 30 * time.Millisecond
}

func TestTerminationReportsErrorWhenUploadsStall(t *testing.T) {
	m, stopper := newTestManager(t)
	stalling(m)
	disks := &fakeDisker{units: []string{"d1"}, uploads: map[string]disk.Uploads{
		"d1": {InProgress: 1, Queued: 2, Errored: 3, OutOfSpace: true, Sent: 100, Errors: 7, LastError: "AccessDenied"},
	}}

	if state, _ := m.HandleTermination(context.Background(), disks, Hooks{}); state.ObservedState != StateSyncing {
		t.Fatalf("first look must keep syncing, got %+v", state)
	}
	time.Sleep(40 * time.Millisecond)
	state, err := m.HandleTermination(context.Background(), disks, Hooks{})

	if err != nil || state.ObservedState != StateError || state.LastError == nil {
		t.Fatalf("stalled uploads must be reported as error, got %+v, %v", state, err)
	}
	for _, part := range []string{"1", "2", "3", "AccessDenied"} {
		if !strings.Contains(*state.LastError, part) {
			t.Fatalf("last_error must describe the queue, missing %q: %s", part, *state.LastError)
		}
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped with a dirty cache: %v", stopper.stopped)
	}
	if again, _ := m.HandleTermination(context.Background(), disks, Hooks{}); again.ObservedState != StateError {
		t.Fatalf("error must stay, got %+v", again)
	}
	if m.CurrentState() != StateError {
		t.Fatalf("phase reports must carry the error, state file says %s", m.CurrentState())
	}
}

func TestTerminationKeepsSyncingWhileUploadsMove(t *testing.T) {
	m, _ := newTestManager(t)
	stalling(m)
	up := disk.Uploads{InProgress: 1, Sent: 0}
	disks := &fakeDisker{units: []string{"d1"}, uploads: map[string]disk.Uploads{"d1": up}}

	for i := 0; i < 8; i++ {
		up.Sent += 1 << 20
		disks.uploads["d1"] = up
		state, _ := m.HandleTermination(context.Background(), disks, Hooks{})
		if state.ObservedState != StateSyncing {
			t.Fatalf("moving upload reported as %+v", state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFailingUploadsDoNotCountAsMovement(t *testing.T) {
	m, _ := newTestManager(t)
	stalling(m)
	up := disk.Uploads{InProgress: 1}
	disks := &fakeDisker{units: []string{"d1"}, uploads: map[string]disk.Uploads{"d1": up}}

	var state client.StatusLifecycle
	for i := 0; i < 8; i++ {
		up.Sent += 1 << 20
		up.Errors++
		disks.uploads["d1"] = up
		state, _ = m.HandleTermination(context.Background(), disks, Hooks{})
		time.Sleep(10 * time.Millisecond)
	}
	if state.ObservedState != StateError {
		t.Fatalf("retries that keep failing are a stall, got %+v", state)
	}
}

func TestUnknownUploadsStallToo(t *testing.T) {
	m, _ := newTestManager(t)
	stalling(m)
	disks := &fakeDisker{units: []string{"d1"}, err: errors.New("connection refused")}

	m.HandleTermination(context.Background(), disks, Hooks{})
	time.Sleep(40 * time.Millisecond)
	state, _ := m.HandleTermination(context.Background(), disks, Hooks{})

	if state.ObservedState != StateError || state.LastError == nil {
		t.Fatalf("rclone that never answers must end in error, got %+v", state)
	}
}

func TestStalledUploadsThatFinishAreSynced(t *testing.T) {
	m, stopper := newTestManager(t)
	stalling(m)
	disks := &fakeDisker{units: []string{"d1"}, pending: map[string]int{"d1": 1}}
	m.HandleTermination(context.Background(), disks, Hooks{})
	time.Sleep(40 * time.Millisecond)
	if state, _ := m.HandleTermination(context.Background(), disks, Hooks{}); state.ObservedState != StateError {
		t.Fatalf("want error, got %+v", state)
	}

	disks.pending["d1"] = 0
	state, err := m.HandleTermination(context.Background(), disks, Hooks{})

	if err != nil || state.ObservedState != StateSynced || len(stopper.stopped) != 1 {
		t.Fatalf("drained cache must be synced, got %+v %v stopped=%v", state, err, stopper.stopped)
	}
}

type fakeDocker struct {
	order    *[]string
	alive    []string
	stubborn bool
}

func (d *fakeDocker) Run(_ context.Context, _ time.Duration, name string, args ...string) (string, error) {
	if name != "docker" {
		return "", nil
	}
	*d.order = append(*d.order, "docker "+args[0])
	switch args[0] {
	case "ps":
		return strings.Join(d.alive, "\n"), nil
	case "stop", "kill":
		if !d.stubborn {
			d.alive = nil
		}
	}
	return "", nil
}

type orderStopper struct{ order *[]string }

func (o orderStopper) Stop(_ context.Context, unit string) error {
	*o.order = append(*o.order, "unmount "+unit)
	return nil
}
func (o orderStopper) Poweroff(context.Context) error { return nil }

func hooksInto(order *[]string, done bool, stopErr *error) Hooks {
	return Hooks{
		AfterStop: func(_ context.Context, err error) bool {
			*order = append(*order, "save")
			if stopErr != nil {
				*stopErr = err
			}
			return done
		},
	}
}

func TestTerminationSavesStateBetweenStopAndUnmount(t *testing.T) {
	var order []string
	docker := &fakeDocker{order: &order, alive: []string{"abc"}}
	m := NewManager(t.TempDir(), orderStopper{&order}, docker, slog.New(slog.NewTextHandler(io.Discard, nil)))

	state, err := m.HandleTermination(context.Background(), &fakeDisker{units: []string{"d1"}}, hooksInto(&order, true, nil))

	if err != nil || state.ObservedState != StateSynced {
		t.Fatalf("state = %+v, %v", state, err)
	}
	want := []string{"docker ps", "docker stop", "docker ps", "save", "unmount storage-mount-d1.service"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
}

func TestRecoveredTerminationStopsContainersBeforeSaving(t *testing.T) {
	var order []string
	docker := &fakeDocker{order: &order, alive: []string{"abc"}}
	m := NewManager(t.TempDir(), orderStopper{&order}, docker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := m.SetState(StateSyncing); err != nil {
		t.Fatal(err)
	}

	if _, err := m.HandleTermination(context.Background(), &fakeDisker{}, hooksInto(&order, true, nil)); err != nil {
		t.Fatal(err)
	}
	want := []string{"docker ps", "docker stop", "docker ps", "save"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
}

func TestTerminationKillsContainerThatIgnoresStop(t *testing.T) {
	var order []string
	docker := &fakeDocker{order: &order, alive: []string{"abc"}}
	m := NewManager(t.TempDir(), orderStopper{&order}, &stopIgnored{docker}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var stopErr error

	state, _ := m.HandleTermination(context.Background(), &fakeDisker{}, hooksInto(&order, true, &stopErr))

	if state.ObservedState != StateSynced || stopErr != nil {
		t.Fatalf("state=%+v stopErr=%v", state, stopErr)
	}
	want := []string{"docker ps", "docker stop", "docker ps", "docker kill", "docker ps", "save"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
}

type stopIgnored struct{ *fakeDocker }

func (s *stopIgnored) Run(ctx context.Context, d time.Duration, name string, args ...string) (string, error) {
	if name == "docker" && args[0] == "stop" {
		*s.order = append(*s.order, "docker stop")
		return "", nil
	}
	return s.fakeDocker.Run(ctx, d, name, args...)
}

func TestTerminationReportsContainerThatSurvivesKill(t *testing.T) {
	var order []string
	docker := &fakeDocker{order: &order, alive: []string{"abc"}, stubborn: true}
	m := NewManager(t.TempDir(), orderStopper{&order}, docker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var stopErr error

	m.HandleTermination(context.Background(), &fakeDisker{}, hooksInto(&order, false, &stopErr))

	if stopErr == nil || !strings.Contains(stopErr.Error(), "контейнер не остановился") {
		t.Fatalf("save must learn the container is alive, got %v", stopErr)
	}
}

func TestTerminationKeepsDisksWhileSaveRetries(t *testing.T) {
	var order []string
	docker := &fakeDocker{order: &order}
	m := NewManager(t.TempDir(), orderStopper{&order}, docker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	disks := &fakeDisker{units: []string{"d1"}}

	state, err := m.HandleTermination(context.Background(), disks, hooksInto(&order, false, nil))
	if err != nil || state.ObservedState != StateSyncing {
		t.Fatalf("state = %+v, %v; want syncing", state, err)
	}
	for _, step := range order {
		if strings.HasPrefix(step, "unmount") {
			t.Fatalf("disk flushed while the save is still retrying: %v", order)
		}
	}

	order = nil
	state, err = m.HandleTermination(context.Background(), disks, hooksInto(&order, true, nil))
	if err != nil || state.ObservedState != StateSynced {
		t.Fatalf("state = %+v, %v; want synced", state, err)
	}
	want := []string{"docker ps", "save", "unmount storage-mount-d1.service"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
}

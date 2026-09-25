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
)

type fakeDisker struct {
	units   []string
	pending map[string]int
	err     error
}

func (f *fakeDisker) ListUnits() ([]string, error) { return f.units, nil }
func (f *fakeDisker) PendingUploads(_ context.Context, id string) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.pending[id], nil
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
	disk := &fakeDisker{units: []string{"d1"}, pending: map[string]int{"d1": 2}}

	state, err := m.HandleTermination(context.Background(), disk, Hooks{})
	if err != nil || state != StateSyncing {
		t.Fatalf("state = %q, %v; want syncing", state, err)
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped with uploads pending: %v", stopper.stopped)
	}

	disk.pending["d1"] = 0
	state, err = m.HandleTermination(context.Background(), disk, Hooks{})
	if err != nil || state != StateSynced {
		t.Fatalf("state = %q, %v; want synced", state, err)
	}
	if want := []string{"storage-mount-d1.service"}; !reflect.DeepEqual(stopper.stopped, want) {
		t.Fatalf("stopped = %v; want %v", stopper.stopped, want)
	}
}

func TestTerminationStopsMountsRightAwayWhenNothingPending(t *testing.T) {
	m, stopper := newTestManager(t)

	state, err := m.HandleTermination(context.Background(), &fakeDisker{units: []string{"d1", "d2"}}, Hooks{})
	if err != nil || state != StateSynced {
		t.Fatalf("state = %q, %v; want synced", state, err)
	}
	if want := []string{"storage-mount-d1.service", "storage-mount-d2.service"}; !reflect.DeepEqual(stopper.stopped, want) {
		t.Fatalf("stopped = %v; want %v", stopper.stopped, want)
	}
}

func TestTerminationKeepsSyncingWhenUploadsUnknown(t *testing.T) {
	m, stopper := newTestManager(t)

	state, _ := m.HandleTermination(context.Background(), &fakeDisker{units: []string{"d1"}, err: errors.New("rc timeout")}, Hooks{})
	if state != StateSyncing {
		t.Fatalf("state = %q; want syncing", state)
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped while uploads unknown: %v", stopper.stopped)
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
		BeforeStop: func(context.Context) { *order = append(*order, "freeze") },
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

	if err != nil || state != StateSynced {
		t.Fatalf("state = %q, %v", state, err)
	}
	want := []string{"freeze", "docker ps", "docker stop", "docker ps", "save", "unmount storage-mount-d1.service"}
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

	if state != StateSynced || stopErr != nil {
		t.Fatalf("state=%s stopErr=%v", state, stopErr)
	}
	want := []string{"freeze", "docker ps", "docker stop", "docker ps", "docker kill", "docker ps", "save"}
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
	disk := &fakeDisker{units: []string{"d1"}}

	state, err := m.HandleTermination(context.Background(), disk, hooksInto(&order, false, nil))
	if err != nil || state != StateSyncing {
		t.Fatalf("state = %q, %v; want syncing", state, err)
	}
	for _, step := range order {
		if strings.HasPrefix(step, "unmount") {
			t.Fatalf("disk flushed while the save is still retrying: %v", order)
		}
	}

	order = nil
	state, err = m.HandleTermination(context.Background(), disk, hooksInto(&order, true, nil))
	if err != nil || state != StateSynced {
		t.Fatalf("state = %q, %v; want synced", state, err)
	}
	want := []string{"docker ps", "save", "unmount storage-mount-d1.service"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
}

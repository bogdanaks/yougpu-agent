package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
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

	state, err := m.HandleTermination(context.Background(), disk)
	if err != nil || state != StateSyncing {
		t.Fatalf("state = %q, %v; want syncing", state, err)
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped with uploads pending: %v", stopper.stopped)
	}

	disk.pending["d1"] = 0
	state, err = m.HandleTermination(context.Background(), disk)
	if err != nil || state != StateSynced {
		t.Fatalf("state = %q, %v; want synced", state, err)
	}
	if want := []string{"storage-mount-d1.service"}; !reflect.DeepEqual(stopper.stopped, want) {
		t.Fatalf("stopped = %v; want %v", stopper.stopped, want)
	}
}

func TestTerminationStopsMountsRightAwayWhenNothingPending(t *testing.T) {
	m, stopper := newTestManager(t)

	state, err := m.HandleTermination(context.Background(), &fakeDisker{units: []string{"d1", "d2"}})
	if err != nil || state != StateSynced {
		t.Fatalf("state = %q, %v; want synced", state, err)
	}
	if want := []string{"storage-mount-d1.service", "storage-mount-d2.service"}; !reflect.DeepEqual(stopper.stopped, want) {
		t.Fatalf("stopped = %v; want %v", stopper.stopped, want)
	}
}

func TestTerminationKeepsSyncingWhenUploadsUnknown(t *testing.T) {
	m, stopper := newTestManager(t)

	state, _ := m.HandleTermination(context.Background(), &fakeDisker{units: []string{"d1"}, err: errors.New("rc timeout")})
	if state != StateSyncing {
		t.Fatalf("state = %q; want syncing", state)
	}
	if len(stopper.stopped) != 0 {
		t.Fatalf("mount stopped while uploads unknown: %v", stopper.stopped)
	}
}

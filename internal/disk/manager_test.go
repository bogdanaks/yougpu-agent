package disk

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

type fakeSystemd struct {
	mu       sync.Mutex
	active   map[string]bool
	calls    []string
	startErr error
}

func newFakeSystemd() *fakeSystemd {
	return &fakeSystemd{active: map[string]bool{}}
}

func (f *fakeSystemd) record(c string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *fakeSystemd) DaemonReload(context.Context) error { f.record("daemon-reload"); return nil }
func (f *fakeSystemd) Enable(_ context.Context, u string) error {
	f.record("enable:" + u)
	return nil
}
func (f *fakeSystemd) Disable(_ context.Context, u string) error {
	f.record("disable:" + u)
	return nil
}
func (f *fakeSystemd) Start(_ context.Context, u string) error {
	f.record("start:" + u)
	if f.startErr != nil {
		return f.startErr
	}
	f.mu.Lock()
	f.active[u] = true
	f.mu.Unlock()
	return nil
}
func (f *fakeSystemd) Stop(_ context.Context, u string) error {
	f.record("stop:" + u)
	f.mu.Lock()
	delete(f.active, u)
	f.mu.Unlock()
	return nil
}
func (f *fakeSystemd) Restart(_ context.Context, u string) error {
	f.record("restart:" + u)
	f.mu.Lock()
	f.active[u] = true
	f.mu.Unlock()
	return nil
}
func (f *fakeSystemd) IsActive(_ context.Context, u string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active[u], nil
}
func (f *fakeSystemd) Poweroff(context.Context) error { f.record("poweroff"); return nil }

type fakeExec struct{}

func (fakeExec) Run(context.Context, time.Duration, string, ...string) (string, error) {
	return "Filesystem     1G-blocks  Used Available Use% Mounted on\n/dev/sda1            100G   10G       100G   10% /\n", nil
}

func newTestManager(t *testing.T) (*Manager, *fakeSystemd, string) {
	t.Helper()
	tmp := t.TempDir()
	sd := newFakeSystemd()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := NewManager(sd, fakeExec{}, log)
	m.SetUnitsDir(tmp)
	return m, sd, tmp
}

func TestMountWritesUnitAndStarts(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	spec := client.AgentDiskSpec{
		ID:           "abc",
		DesiredState: client.DesiredMounted,
		Bucket:       "test-bucket",
		S3Path:       "u/abc/",
		MountPath:    filepath.Join(t.TempDir(), "mount"),
	}

	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatalf("mount: %v", err)
	}

	unitName := "storage-mount-abc.service"
	unitPath := filepath.Join(tmp, unitName)
	body, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	if !bytes.Contains(body, []byte("remote:test-bucket/u/abc/")) {
		t.Errorf("unit missing rclone path:\n%s", body)
	}
	if !bytes.Contains(body, []byte(spec.MountPath)) {
		t.Errorf("unit missing mount path:\n%s", body)
	}

	want := []string{"daemon-reload", "enable:" + unitName, "start:" + unitName}
	for _, c := range want {
		found := false
		for _, got := range sd.calls {
			if got == c {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("systemd call %q not made; calls=%v", c, sd.calls)
		}
	}
}

func TestUnmountRemovesUnit(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	unitName := "storage-mount-xyz.service"
	if err := os.WriteFile(filepath.Join(tmp, unitName), []byte("placeholder"), 0o644); err != nil {
		t.Fatal(err)
	}
	sd.active[unitName] = true

	if err := m.Unmount(context.Background(), "xyz"); err != nil {
		t.Fatalf("unmount: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, unitName)); !os.IsNotExist(err) {
		t.Errorf("unit file still present: %v", err)
	}
	joined := strings.Join(sd.calls, ",")
	if !strings.Contains(joined, "stop:"+unitName) {
		t.Errorf("expected stop call, calls=%v", sd.calls)
	}
}

func TestListUnits(t *testing.T) {
	m, _, tmp := newTestManager(t)
	files := []string{
		"storage-mount-a.service",
		"storage-mount-bbb.service",
		"unrelated.service",
		"storage-mount-x.timer",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tmp, f), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := m.ListUnits()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 ids, got %v", ids)
	}
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	if !set["a"] || !set["bbb"] {
		t.Errorf("unexpected ids: %v", ids)
	}
}

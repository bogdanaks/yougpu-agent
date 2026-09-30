package disk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

type fakeKeys struct {
	mu     sync.Mutex
	prefix string
	asked  []string
	err    error
}

func (f *fakeKeys) GetStorageCredentials(_ context.Context, driveID string) (*client.StorageCredentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, driveID)
	if f.err != nil {
		return nil, f.err
	}
	n := len(f.asked)
	return &client.StorageCredentials{
		Endpoint:     "https://s3.example.com",
		AccessKey:    fmt.Sprintf("AK-%s%s-%d", f.prefix, driveID, n),
		SecretKey:    fmt.Sprintf("SK-%s%s-%d", f.prefix, driveID, n),
		CredentialID: fmt.Sprintf("cid-%d", n),
	}, nil
}

func (f *fakeKeys) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

func keysOf(m *Manager) *fakeKeys { return m.creds.(*fakeKeys) }

type testClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

func rcloneConf(t *testing.T, m *Manager) string {
	t.Helper()
	raw, err := os.ReadFile(m.rcloneConfigPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(raw)
}

func remoteOf(t *testing.T, m *Manager, id string) map[string]string {
	t.Helper()
	var out map[string]string
	for _, line := range strings.Split(rcloneConf(t, m), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			if line == "[disk-"+id+"]" {
				out = map[string]string{}
			} else if out != nil {
				return out
			}
			continue
		}
		if k, v, ok := strings.Cut(line, " = "); ok && out != nil {
			out[k] = v
		}
	}
	return out
}

func TestMountAsksKeyOfItsOwnDriveOnce(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	spec := diskSpec(t, "abc")

	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatalf("mount: %v", err)
	}

	if got := keysOf(m).requests(); len(got) != 1 || got[0] != "abc" {
		t.Fatalf("key requests %v, want one for abc", got)
	}
	remote := remoteOf(t, m, "abc")
	if remote == nil || remote["type"] != "s3" || remote["access_key_id"] != "AK-abc-1" || remote["secret_access_key"] != "SK-abc-1" || remote["endpoint"] != "https://s3.example.com" {
		t.Fatalf("remote of abc: %v\n%s", remote, rcloneConf(t, m))
	}
	info, err := os.Stat(m.rcloneConfigPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rclone.conf must be private: %v %v", info.Mode(), err)
	}
	if exec := execStart(t, unitBody(t, tmp, "abc")); !strings.Contains(exec, " disk-abc:test-bucket/u/abc/ ") {
		t.Fatalf("unit must mount the remote of its drive: %s", exec)
	}

	sd.mu.Lock()
	delete(sd.active, "storage-mount-abc.service")
	sd.mu.Unlock()
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(m).requests(); len(got) != 1 {
		t.Fatalf("remount must reuse the key, requests %v", got)
	}
}

func TestKeySurvivesAgentRestart(t *testing.T) {
	m, _, _ := newTestManager(t)
	spec := diskSpec(t, "abc")
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	restarted, _, _ := newTestManager(t)
	restarted.SetRcloneConfigPath(m.rcloneConfigPath)
	if err := restarted.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	if got := keysOf(restarted).requests(); len(got) != 0 {
		t.Fatalf("key on disk must be used after a restart, requests %v", got)
	}
}

func TestEachDriveHasItsOwnRemote(t *testing.T) {
	m, _, tmp := newTestManager(t)
	for _, id := range []string{"a", "b"} {
		if err := m.Mount(context.Background(), diskSpec(t, id)); err != nil {
			t.Fatal(err)
		}
	}

	a, b := remoteOf(t, m, "a"), remoteOf(t, m, "b")
	if a == nil || b == nil || a["access_key_id"] == b["access_key_id"] {
		t.Fatalf("drives share a key:\n%s", rcloneConf(t, m))
	}
	if !strings.Contains(execStart(t, unitBody(t, tmp, "b")), " disk-b:test-bucket/u/b/ ") {
		t.Fatal("unit of b mounts another remote")
	}
}

func TestDriveWithoutKeyIsNotMounted(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	keysOf(m).err = &client.HTTPError{Status: 400, Body: "drive is not attached"}

	err := m.Mount(context.Background(), diskSpec(t, "abc"))

	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("mount without a key must fail with the reason, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "storage-mount-abc.service")); !os.IsNotExist(err) {
		t.Fatalf("unit written without a key: %v", err)
	}
	if sd.called("start:storage-mount-abc.service") || remoteOf(t, m, "abc") != nil {
		t.Fatal("drive started without a key")
	}
}

func TestUnmountRemovesOnlyItsRemote(t *testing.T) {
	m, _, _ := newTestManager(t)
	for _, id := range []string{"d1", "d2"} {
		if err := m.Mount(context.Background(), diskSpec(t, id)); err != nil {
			t.Fatal(err)
		}
	}
	withRc(t, m, "d1", rcStats(`{"diskCache":{"uploadsInProgress":0,"uploadsQueued":0}}`, `{}`))

	if err := m.Unmount(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}

	if remoteOf(t, m, "d1") != nil {
		t.Fatalf("key of an unmounted drive left behind:\n%s", rcloneConf(t, m))
	}
	if r := remoteOf(t, m, "d2"); r == nil || r["access_key_id"] != "AK-d2-2" {
		t.Fatalf("other drive lost its key:\n%s", rcloneConf(t, m))
	}
}

func TestFlushingDriveKeepsItsRemote(t *testing.T) {
	m, _, _ := newTestManager(t)
	if err := m.Mount(context.Background(), diskSpec(t, "d1")); err != nil {
		t.Fatal(err)
	}
	withRc(t, m, "d1", rcStats(`{"diskCache":{"uploadsInProgress":1,"uploadsQueued":0}}`, `{}`))

	if err := m.Unmount(context.Background(), "d1"); !errors.Is(err, ErrFlushing) {
		t.Fatalf("got %v", err)
	}
	if remoteOf(t, m, "d1") == nil {
		t.Fatal("key removed while the drive uploads its cache")
	}
}

func TestFailedMountAsksNewKeyAtMostEveryTenMinutes(t *testing.T) {
	m, sd, _ := newTestManager(t)
	clk := &testClock{at: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}
	m.now = clk.now
	sd.startErr = errors.New("unit failed")
	spec := diskSpec(t, "abc")

	for range 3 {
		if err := m.Mount(context.Background(), spec); err == nil {
			t.Fatal("mount must fail")
		}
		clk.add(time.Minute)
	}
	if got := keysOf(m).requests(); len(got) != 1 {
		t.Fatalf("key asked again before ten minutes passed: %v", got)
	}

	clk.add(7 * time.Minute)
	if err := m.Mount(context.Background(), spec); err == nil {
		t.Fatal("mount must fail")
	}
	if got := keysOf(m).requests(); len(got) != 2 {
		t.Fatalf("failed mount after ten minutes must ask a new key, requests %v", got)
	}
	if remoteOf(t, m, "abc")["access_key_id"] != "AK-abc-2" {
		t.Fatalf("new key not written:\n%s", rcloneConf(t, m))
	}
}

func TestFailedMountRetriesWithNewKey(t *testing.T) {
	m, _, _ := newTestManager(t)
	spec := diskSpec(t, "abc")
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	restarted, sd2, _ := newTestManager(t)
	restarted.SetRcloneConfigPath(m.rcloneConfigPath)
	keysOf(restarted).prefix = "new-"
	sd2.startCheck = func() error {
		if strings.Contains(rcloneConf(t, restarted), "AK-abc-1") {
			return errors.New("403 Forbidden")
		}
		return nil
	}

	if err := restarted.Mount(context.Background(), spec); err != nil {
		t.Fatalf("mount with a revoked key must retry with a new one: %v", err)
	}
	if got := keysOf(restarted).requests(); len(got) != 1 {
		t.Fatalf("requests %v", got)
	}
}

func TestRemoteWritesDoNotLoseEachOther(t *testing.T) {
	m, _, _ := newTestManager(t)
	var wg sync.WaitGroup
	ids := make([]string, 16)
	for i := range ids {
		ids[i] = fmt.Sprintf("d%d", i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := m.ensureRemote(context.Background(), id); err != nil {
				t.Error(err)
			}
		}(ids[i])
	}
	wg.Wait()

	for _, id := range ids {
		if remoteOf(t, m, id) == nil {
			t.Fatalf("remote of %s lost:\n%s", id, rcloneConf(t, m))
		}
	}
}

func TestDirectModeUsesAndDropsDriveRemote(t *testing.T) {
	m, _, _, exec := newTestManagerExec(t)
	m.SetDirectMode(true)
	spec := diskSpec(t, "abc")

	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	call := exec.find("/usr/bin/rclone")
	if call == nil || call[2] != "disk-abc:test-bucket/u/abc/" || remoteOf(t, m, "abc") == nil {
		t.Fatalf("direct mount must use the drive remote: %v\n%s", call, rcloneConf(t, m))
	}

	if err := m.Unmount(context.Background(), "abc"); err != nil {
		t.Fatal(err)
	}
	if remoteOf(t, m, "abc") != nil {
		t.Fatal("direct unmount left the key behind")
	}
}

func TestUnknownUnitStateDoesNotRenewTheKey(t *testing.T) {
	m, _, _ := newTestManager(t)
	spec := diskSpec(t, "abc")
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	restarted, sd, _ := newTestManager(t)
	restarted.SetRcloneConfigPath(m.rcloneConfigPath)
	sd.activeErr = errors.New("systemctl is-active storage-mount-abc.service: signal: killed")

	if err := restarted.Mount(context.Background(), spec); err == nil {
		t.Fatal("mount with unknown unit state must fail")
	}
	if got := keysOf(restarted).requests(); len(got) != 0 {
		t.Fatalf("key renewed while rclone may still run with the old one: %v", got)
	}
}

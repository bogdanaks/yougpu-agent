package disk

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func writeRcEnv(t *testing.T, m *Manager, id, addr, user, pass string) {
	t.Helper()
	path := filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-"+id+".env")
	body := "RCLONE_RC_ADDR=" + addr + "\nRCLONE_RC_USER=" + user + "\nRCLONE_RC_PASS=" + pass + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func withRc(t *testing.T, m *Manager, id string, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "u-"+id || pass != "p-"+id {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	writeRcEnv(t, m, id, srv.Listener.Addr().String(), "u-"+id, "p-"+id)
}

func rcStats(vfs, core string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/vfs/stats":
			_, _ = w.Write([]byte(vfs))
		case "/core/stats":
			_, _ = w.Write([]byte(core))
		default:
			http.NotFound(w, r)
		}
	}
}

func TestUploadsReportsQueueErrorsAndProgress(t *testing.T) {
	m, _, _ := newTestManager(t)
	withRc(t, m, "d1", rcStats(
		`{"diskCache":{"uploadsInProgress":1,"uploadsQueued":2,"erroredFiles":3,"outOfSpace":true}}`,
		`{"bytes":1234,"transfers":5,"errors":6,"lastError":"403 Forbidden"}`,
	))

	up, err := m.Uploads(context.Background(), "d1")
	if err != nil {
		t.Fatal(err)
	}
	want := Uploads{InProgress: 1, Queued: 2, Errored: 3, OutOfSpace: true, Sent: 1234, Completed: 5, Errors: 6, LastError: "403 Forbidden"}
	if up != want || up.Pending() != 3 {
		t.Fatalf("uploads = %+v, want %+v", up, want)
	}
}

func TestUploadsZeroWhenQueueDrained(t *testing.T) {
	m, _, _ := newTestManager(t)
	withRc(t, m, "d1", rcStats(`{"diskCache":{"uploadsInProgress":0,"uploadsQueued":0}}`, `{"bytes":10}`))
	up, err := m.Uploads(context.Background(), "d1")
	if err != nil || up.Pending() != 0 {
		t.Fatalf("pending = %d, %v; want 0", up.Pending(), err)
	}
}

func TestUploadsZeroWithoutWriteCache(t *testing.T) {
	m, _, _ := newTestManager(t)
	withRc(t, m, "d1", rcStats(`{}`, `{}`))
	up, err := m.Uploads(context.Background(), "d1")
	if err != nil || up.Pending() != 0 {
		t.Fatalf("pending = %d, %v; want 0", up.Pending(), err)
	}
}

func TestUploadsUnknownWhenRcloneNotRunning(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	m, _, _ := newTestManager(t)
	writeRcEnv(t, m, "d1", addr, "u", "p")

	if up, err := m.Uploads(context.Background(), "d1"); err == nil {
		t.Fatalf("unreachable rc must be unknown, not %d pending", up.Pending())
	}
}

func TestUploadsUnknownWithoutRcAccess(t *testing.T) {
	m, _, _ := newTestManager(t)
	if _, err := m.Uploads(context.Background(), "d1"); err == nil {
		t.Fatal("disk without rc access must be unknown")
	}
}

func TestUploadsRequireRcPassword(t *testing.T) {
	m, _, _ := newTestManager(t)
	withRc(t, m, "d1", rcStats(`{"diskCache":{"uploadsInProgress":0,"uploadsQueued":0}}`, `{}`))
	env, _ := readEnv(t, m, "d1")
	writeRcEnv(t, m, "d1", env["RCLONE_RC_ADDR"], "u-d1", "wrong")

	if _, err := m.Uploads(context.Background(), "d1"); err == nil {
		t.Fatal("rc answered without the right password")
	}
}

func TestUploadsZeroInDirectMode(t *testing.T) {
	m, _, _ := newTestManager(t)
	m.SetDirectMode(true)
	up, err := m.Uploads(context.Background(), "d1")
	if err != nil || up.Pending() != 0 {
		t.Fatalf("direct mode has no rc: pending = %d, %v", up.Pending(), err)
	}
}

func TestUploadsErrorOnBadAnswer(t *testing.T) {
	m, _, _ := newTestManager(t)
	withRc(t, m, "d1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	if _, err := m.Uploads(context.Background(), "d1"); err == nil {
		t.Fatal("want error on rc failure")
	}
}

func TestCredentialsHotReloadUsesRcPassword(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	if err := os.WriteFile(filepath.Join(tmp, "storage-mount-d1.service"), []byte("unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	var got atomic.Value
	withRc(t, m, "d1", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/config/update" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Parameters map[string]string `json:"parameters"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		got.Store(body.Parameters["access_key_id"])
	})

	err := m.ApplyCredentials(context.Background(), &client.StorageCredentials{AccessKey: "AK2", SecretKey: "SK2", Endpoint: "https://s3", ExpiresAt: time.Now().Add(time.Hour)})

	if err != nil {
		t.Fatal(err)
	}
	if got.Load() != "AK2" {
		t.Fatalf("hot reload did not reach rc, got %v", got.Load())
	}
	if sd.called("restart:storage-mount-d1.service") {
		t.Fatal("unit restarted although rc accepted the new key")
	}
}

func activeUnit(t *testing.T, m *Manager, sd *fakeSystemd, dir, id string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "storage-mount-"+id+".service"), []byte("unit"), 0o644); err != nil {
		t.Fatal(err)
	}
	sd.mu.Lock()
	sd.active["storage-mount-"+id+".service"] = true
	sd.mu.Unlock()
}

func TestUnmountWaitsForPendingUploads(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	activeUnit(t, m, sd, tmp, "d1")
	var pending atomic.Int32
	pending.Store(2)
	withRc(t, m, "d1", func(w http.ResponseWriter, r *http.Request) {
		rcStats(`{"diskCache":{"uploadsInProgress":`+strconv.Itoa(int(pending.Load()))+`,"uploadsQueued":0}}`, `{}`)(w, r)
	})

	err := m.Unmount(context.Background(), "d1")
	if !errors.Is(err, ErrFlushing) {
		t.Fatalf("unmount with uploads in flight must wait, got %v", err)
	}
	if sd.called("stop:storage-mount-d1.service") {
		t.Fatal("unit stopped with a dirty cache")
	}
	if _, err := os.Stat(filepath.Join(tmp, "storage-mount-d1.service")); err != nil {
		t.Fatal("unit removed while uploads are in flight")
	}

	pending.Store(0)
	if err := m.Unmount(context.Background(), "d1"); err != nil {
		t.Fatalf("drained cache must unmount: %v", err)
	}
	if !sd.called("stop:storage-mount-d1.service") {
		t.Fatal("unit not stopped after the cache drained")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-d1.env")); !os.IsNotExist(err) {
		t.Fatalf("rc env file left behind: %v", err)
	}
}

func TestUnmountWaitsWhileUploadsAreUnknown(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	activeUnit(t, m, sd, tmp, "d1")

	if err := m.Unmount(context.Background(), "d1"); !errors.Is(err, ErrFlushing) {
		t.Fatalf("unknown uploads must keep the disk, got %v", err)
	}
	if sd.called("stop:storage-mount-d1.service") {
		t.Fatal("unit stopped while uploads are unknown")
	}
}

func TestUnmountStopsWaitingAfterLimit(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	m.flushLimit = 30 * time.Millisecond
	activeUnit(t, m, sd, tmp, "d1")
	withRc(t, m, "d1", rcStats(`{"diskCache":{"uploadsInProgress":1,"uploadsQueued":0}}`, `{}`))

	if err := m.Unmount(context.Background(), "d1"); !errors.Is(err, ErrFlushing) {
		t.Fatalf("first attempt must wait, got %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := m.Unmount(context.Background(), "d1"); err != nil {
		t.Fatalf("after the limit the disk is unmounted anyway: %v", err)
	}
	if !sd.called("stop:storage-mount-d1.service") {
		t.Fatal("unit not stopped after the limit")
	}
}

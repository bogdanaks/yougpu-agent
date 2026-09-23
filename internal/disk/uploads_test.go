package disk

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func managerWithRc(t *testing.T, id string, handler http.HandlerFunc) *Manager {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	_, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	m, _, _ := newTestManager(t)
	m.SetRcPortBase(port - (m.RcPortFor(id) - m.rcPortBase))
	return m
}

func stats(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/vfs/stats" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func TestPendingUploadsCountsQueuedAndInProgress(t *testing.T) {
	m := managerWithRc(t, "d1", stats(`{"diskCache":{"uploadsInProgress":1,"uploadsQueued":2}}`))
	n, err := m.PendingUploads(context.Background(), "d1")
	if err != nil || n != 3 {
		t.Fatalf("pending = %d, %v; want 3", n, err)
	}
}

func TestPendingUploadsZeroWhenQueueDrained(t *testing.T) {
	m := managerWithRc(t, "d1", stats(`{"diskCache":{"uploadsInProgress":0,"uploadsQueued":0}}`))
	n, err := m.PendingUploads(context.Background(), "d1")
	if err != nil || n != 0 {
		t.Fatalf("pending = %d, %v; want 0", n, err)
	}
}

func TestPendingUploadsZeroWithoutWriteCache(t *testing.T) {
	m := managerWithRc(t, "d1", stats(`{}`))
	n, err := m.PendingUploads(context.Background(), "d1")
	if err != nil || n != 0 {
		t.Fatalf("pending = %d, %v; want 0", n, err)
	}
}

func TestPendingUploadsZeroWhenRcloneNotRunning(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	m, _, _ := newTestManager(t)
	m.SetRcPortBase(port - (m.RcPortFor("d1") - m.rcPortBase))

	n, err := m.PendingUploads(context.Background(), "d1")
	if err != nil || n != 0 {
		t.Fatalf("pending = %d, %v; want 0", n, err)
	}
}

func TestPendingUploadsErrorOnBadAnswer(t *testing.T) {
	m := managerWithRc(t, "d1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	if _, err := m.PendingUploads(context.Background(), "d1"); err == nil {
		t.Fatal("want error on rc failure")
	}
}

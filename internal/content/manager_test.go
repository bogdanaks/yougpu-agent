package content

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func newTestManager() *Manager { return New(slog.New(slog.NewTextHandler(io.Discard, nil))) }

func containerWith(root string) *client.AgentContainerSpec {
	return &client.AgentContainerSpec{
		Volumes: []client.ContainerVolume{{Host: root, Container: WorkspaceContainerPath}},
	}
}

func TestWorkspaceRoot(t *testing.T) {
	if got := WorkspaceRoot(containerWith("/root/workspace")); got != "/root/workspace" {
		t.Fatalf("want /root/workspace, got %q", got)
	}
	if got := WorkspaceRoot(nil); got != "" {
		t.Fatalf("want empty for nil container, got %q", got)
	}
	noWs := &client.AgentContainerSpec{Volumes: []client.ContainerVolume{{Host: "/x", Container: "/data"}}}
	if got := WorkspaceRoot(noWs); got != "" {
		t.Fatalf("want empty when no /workspace volume, got %q", got)
	}
}

func TestReconcileInlineFileAndModelDownload(t *testing.T) {
	root := t.TempDir()
	body := []byte("fake-model-weights")
	sum := sha256.Sum256(body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	spec := &client.AgentContentSpec{
		WorkspaceFiles: []client.ContentFile{{Content: `{"a":1}`, Dest: "user/default/workflows", Name: "wf.json"}},
		Models: []client.ContentModel{
			{URL: srv.URL + "/m.safetensors", Type: "checkpoints", Name: "m.safetensors", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body))},
		},
	}

	obs := newTestManager().Reconcile(context.Background(), spec, containerWith(root))
	if obs.ObservedState != client.ContentReady {
		t.Fatalf("want ready, got %s (err=%v)", obs.ObservedState, obs.LastError)
	}

	wf, err := os.ReadFile(filepath.Join(root, "user/default/workflows/wf.json"))
	if err != nil || string(wf) != `{"a":1}` {
		t.Fatalf("workflow not written: %v %q", err, wf)
	}
	mdl, err := os.ReadFile(filepath.Join(root, "models/checkpoints/m.safetensors"))
	if err != nil || string(mdl) != string(body) {
		t.Fatalf("model not written: %v", err)
	}
}

func TestReconcileDedupSkipsPresent(t *testing.T) {
	root := t.TempDir()
	body := []byte("weights")
	sum := sha256.Sum256(body)
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	spec := &client.AgentContentSpec{
		Models: []client.ContentModel{{URL: srv.URL + "/m", Type: "vae", Name: "m.bin", SHA256: hex.EncodeToString(sum[:])}},
	}
	mgr := newTestManager()
	if obs := mgr.Reconcile(context.Background(), spec, containerWith(root)); obs.ObservedState != client.ContentReady {
		t.Fatalf("first pass not ready: %s", obs.ObservedState)
	}
	if obs := mgr.Reconcile(context.Background(), spec, containerWith(root)); obs.ObservedState != client.ContentReady {
		t.Fatalf("second pass not ready: %s", obs.ObservedState)
	}
	if hits != 1 {
		t.Fatalf("expected 1 download (dedup), got %d", hits)
	}
}

func TestReconcileSHA256Mismatch(t *testing.T) {
	root := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("actual"))
	}))
	defer srv.Close()

	spec := &client.AgentContentSpec{
		Models: []client.ContentModel{{URL: srv.URL + "/m", Type: "vae", Name: "m.bin", SHA256: "deadbeef"}},
	}
	obs := newTestManager().Reconcile(context.Background(), spec, containerWith(root))
	if obs.ObservedState != client.ContentError {
		t.Fatalf("want error on sha mismatch, got %s", obs.ObservedState)
	}
	if _, err := os.Stat(filepath.Join(root, "models/vae/m.bin")); !os.IsNotExist(err) {
		t.Fatalf("mismatched file should not be persisted")
	}
}

func randomBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func modelSpec(url string, body []byte) *client.AgentContentSpec {
	sum := sha256.Sum256(body)
	return &client.AgentContentSpec{
		Models: []client.ContentModel{{URL: url, Type: "vae", Name: "m.bin", SHA256: hex.EncodeToString(sum[:])}},
	}
}

func TestFetchRangedMatchesSingleStream(t *testing.T) {
	root := t.TempDir()
	body := randomBody(1 << 20)

	var mu sync.Mutex
	var rangeGets int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rng := r.Header.Get("Range"); rng != "" && !strings.HasSuffix(rng, "-") {
			mu.Lock()
			rangeGets++
			mu.Unlock()
		}
		http.ServeContent(w, r, "m.bin", time.Time{}, bytes.NewReader(body))
	}))
	defer srv.Close()

	mgr := newTestManager()
	mgr.SetRangeMinForTest(1024)
	obs := mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(root))
	if obs.ObservedState != client.ContentReady {
		t.Fatalf("want ready, got %s (err=%v)", obs.ObservedState, obs.LastError)
	}

	got, err := os.ReadFile(filepath.Join(root, "models/vae/m.bin"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("ranged download differs from source (got %d bytes, want %d)", len(got), len(body))
	}
	if rangeGets != rangeParts {
		t.Errorf("expected %d ranged GETs, got %d", rangeParts, rangeGets)
	}
}

func TestFetchFallsBackWithoutRangeSupport(t *testing.T) {
	root := t.TempDir()
	body := randomBody(64 << 10)

	var mu sync.Mutex
	var gets int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		gets++
		mu.Unlock()
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	mgr := newTestManager()
	mgr.SetRangeMinForTest(1024)
	obs := mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(root))
	if obs.ObservedState != client.ContentReady {
		t.Fatalf("want ready, got %s (err=%v)", obs.ObservedState, obs.LastError)
	}

	got, err := os.ReadFile(filepath.Join(root, "models/vae/m.bin"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("fallback download broken: err=%v len=%d", err, len(got))
	}
	if gets != 1 {
		t.Errorf("server ignoring Range must be fetched in a single request, got %d", gets)
	}
}

func TestFetchRejectsTruncatedRangeResponse(t *testing.T) {
	root := t.TempDir()
	body := randomBody(8 << 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		half := body[:len(body)/2]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(half)-1, len(body)))
		w.Header().Set("Content-Length", strconv.Itoa(len(half)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(half)
	}))
	defer srv.Close()

	mgr := newTestManager()
	mgr.SetRangeMinForTest(1 << 20)
	obs := mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(root))
	if obs.ObservedState != client.ContentError {
		t.Fatalf("truncated range response must fail, got %s", obs.ObservedState)
	}
	if _, err := os.Stat(filepath.Join(root, "models/vae/m.bin")); !os.IsNotExist(err) {
		t.Error("truncated file must not be persisted")
	}
}

func TestFetchRangedResumesBrokenPart(t *testing.T) {
	root := t.TempDir()
	body := randomBody(1 << 20)

	var mu sync.Mutex
	seen := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		from, to := int64(0), int64(len(body)-1)
		if rng != "bytes=0-" {
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &from, &to); err != nil {
				t.Errorf("unexpected range header %q", rng)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		mu.Lock()
		first := !seen[rng]
		seen[rng] = true
		mu.Unlock()

		chunk := body[from : to+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(body)))
		w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
		w.WriteHeader(http.StatusPartialContent)
		if first && from == 0 && rng != "bytes=0-" {
			_, _ = w.Write(chunk[:len(chunk)/2])
			panic(http.ErrAbortHandler)
		}
		_, _ = w.Write(chunk)
	}))
	defer srv.Close()

	mgr := newTestManager()
	mgr.SetRangeMinForTest(1024)
	obs := mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(root))
	if obs.ObservedState != client.ContentReady {
		t.Fatalf("want ready after resume, got %s (err=%v)", obs.ObservedState, obs.LastError)
	}

	got, err := os.ReadFile(filepath.Join(root, "models/vae/m.bin"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("resumed file differs from source: err=%v len=%d", err, len(got))
	}
}

func TestReconcileNoWorkspaceVolume(t *testing.T) {
	spec := &client.AgentContentSpec{Models: []client.ContentModel{{URL: "http://x/m", Type: "vae", Name: "m"}}}
	obs := newTestManager().Reconcile(context.Background(), spec, &client.AgentContainerSpec{})
	if obs.ObservedState != client.ContentError {
		t.Fatalf("want error without /workspace volume, got %s", obs.ObservedState)
	}
}

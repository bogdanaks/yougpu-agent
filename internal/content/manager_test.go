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
	"sync/atomic"
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

func TestReconcileReplacesZeroSizePlaceholder(t *testing.T) {
	root := t.TempDir()
	placeholder := filepath.Join(root, "models", "vae", "m.bin")
	if err := os.MkdirAll(filepath.Dir(placeholder), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(placeholder, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	body := []byte("weights")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	spec := &client.AgentContentSpec{
		Models: []client.ContentModel{{URL: srv.URL + "/m", Type: "vae", Name: "m.bin"}},
	}
	if obs := newTestManager().Reconcile(context.Background(), spec, containerWith(root)); obs.ObservedState != client.ContentReady {
		t.Fatalf("not ready: %s", obs.ObservedState)
	}

	got, err := os.ReadFile(placeholder)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("плейсхолдер не заменён настоящими весами: %q", got)
	}
}

func gatedServer(hits *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "Access to model is restricted", http.StatusUnauthorized)
	}))
}

func TestReconcileGatedModelReportsUnauthorized(t *testing.T) {
	root := t.TempDir()
	var hits atomic.Int64
	srv := gatedServer(&hits)
	defer srv.Close()

	spec := &client.AgentContentSpec{
		Models: []client.ContentModel{{URL: srv.URL + "/flux.safetensors", Type: "diffusion_models", Name: "flux.safetensors"}},
	}
	obs := newTestManager().Reconcile(context.Background(), spec, containerWith(root))

	if obs.ObservedState != client.ContentError {
		t.Fatalf("want error, got %s", obs.ObservedState)
	}
	if obs.LastError == nil || *obs.LastError != "flux.safetensors: http 401" {
		t.Fatalf("want %q, got %v", "flux.safetensors: http 401", obs.LastError)
	}
	dir := filepath.Join(root, "models", "diffusion_models")
	for _, name := range []string{"flux.safetensors", "flux.safetensors.part"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s must not exist after 401, stat err: %v", name, err)
		}
	}
}

func TestReconcileGatedModelDoesNotBlockOthers(t *testing.T) {
	root := t.TempDir()
	body := []byte("public weights")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/gated.safetensors") || strings.HasSuffix(r.URL.Path, "/gone.safetensors") {
			code := http.StatusUnauthorized
			if strings.HasSuffix(r.URL.Path, "/gone.safetensors") {
				code = http.StatusNotFound
			}
			http.Error(w, "nope", code)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	spec := &client.AgentContentSpec{
		Models: []client.ContentModel{
			{URL: srv.URL + "/gated.safetensors", Type: "text_encoders", Name: "gated.safetensors"},
			{URL: srv.URL + "/public.safetensors", Type: "diffusion_models", Name: "public.safetensors"},
			{URL: srv.URL + "/gone.safetensors", Type: "vae", Name: "gone.safetensors"},
		},
	}
	obs := newTestManager().Reconcile(context.Background(), spec, containerWith(root))

	if obs.ObservedState != client.ContentError {
		t.Fatalf("want error, got %s", obs.ObservedState)
	}
	want := "gated.safetensors: http 401; gone.safetensors: http 404"
	if obs.LastError == nil || *obs.LastError != want {
		t.Fatalf("want %q, got %v", want, obs.LastError)
	}
	got, err := os.ReadFile(filepath.Join(root, "models", "diffusion_models", "public.safetensors"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("public model must be downloaded despite the failed ones: %v %q", err, got)
	}
}

func TestReconcileGatedModelPlacedByUser(t *testing.T) {
	root := t.TempDir()
	var hits atomic.Int64
	srv := gatedServer(&hits)
	defer srv.Close()

	spec := &client.AgentContentSpec{
		Models: []client.ContentModel{{URL: srv.URL + "/flux.safetensors", Type: "diffusion_models", Name: "flux.safetensors"}},
	}
	m := newTestManager()
	if obs := m.Reconcile(context.Background(), spec, containerWith(root)); obs.ObservedState != client.ContentError {
		t.Fatalf("first pass: want error, got %s", obs.ObservedState)
	}

	target := filepath.Join(root, "models", "diffusion_models", "flux.safetensors")
	if err := os.WriteFile(target, []byte("weights from the user"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := hits.Load()

	if obs := m.Reconcile(context.Background(), spec, containerWith(root)); obs.ObservedState != client.ContentReady {
		t.Fatalf("after the user placed the file: want ready, got %s (%v)", obs.ObservedState, obs.LastError)
	}
	if extra := hits.Load() - before; extra != 0 {
		t.Errorf("model is in place, but the agent went to the link again (%d extra request(s))", extra)
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

func silent(r *http.Request, release <-chan struct{}) {
	select {
	case <-r.Context().Done():
	case <-release:
	}
}

func reconcileWithin(t *testing.T, mgr *Manager, spec *client.AgentContentSpec, container *client.AgentContainerSpec) client.AgentContentObserved {
	t.Helper()
	done := make(chan client.AgentContentObserved, 1)
	go func() { done <- mgr.Reconcile(context.Background(), spec, container) }()
	select {
	case obs := <-done:
		return obs
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile hung on a silent connection")
		return client.AgentContentObserved{}
	}
}

func TestFetchRangedRetriesSilentPart(t *testing.T) {
	root := t.TempDir()
	body := randomBody(1 << 20)
	release := make(chan struct{})

	var mu sync.Mutex
	seen := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		from, to := int64(0), int64(len(body)-1)
		if rng != "bytes=0-" {
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &from, &to); err != nil {
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
			w.(http.Flusher).Flush()
			silent(r, release)
			return
		}
		_, _ = w.Write(chunk)
	}))
	defer srv.Close()
	defer close(release)

	mgr := newTestManager()
	mgr.SetRangeMinForTest(1024)
	mgr.SetIdleTimeoutForTest(200 * time.Millisecond)
	obs := reconcileWithin(t, mgr, modelSpec(srv.URL+"/m", body), containerWith(root))
	if obs.ObservedState != client.ContentReady {
		t.Fatalf("want ready after retrying the silent part, got %s (err=%v)", obs.ObservedState, obs.LastError)
	}

	got, err := os.ReadFile(filepath.Join(root, "models/vae/m.bin"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("retried file differs from source: err=%v len=%d", err, len(got))
	}
}

func TestFetchSilentSingleStreamFails(t *testing.T) {
	root := t.TempDir()
	body := randomBody(64 << 10)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		silent(r, release)
	}))
	defer srv.Close()
	defer close(release)

	mgr := newTestManager()
	mgr.SetIdleTimeoutForTest(200 * time.Millisecond)
	obs := reconcileWithin(t, mgr, modelSpec(srv.URL+"/m", body), containerWith(root))
	if obs.ObservedState != client.ContentError {
		t.Fatalf("silent download must fail, got %s", obs.ObservedState)
	}
	if obs.LastError == nil || !strings.Contains(*obs.LastError, "no data for 200ms") {
		t.Errorf("error must name the silence, got %v", obs.LastError)
	}
	if _, err := os.Stat(filepath.Join(root, "models/vae/m.bin")); !os.IsNotExist(err) {
		t.Error("partial file must not be persisted")
	}
}

func TestFetchSilentBeforeHeadersFails(t *testing.T) {
	root := t.TempDir()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		silent(r, release)
	}))
	defer srv.Close()
	defer close(release)

	mgr := newTestManager()
	mgr.SetIdleTimeoutForTest(200 * time.Millisecond)
	obs := reconcileWithin(t, mgr, modelSpec(srv.URL+"/m", randomBody(1024)), containerWith(root))
	if obs.ObservedState != client.ContentError {
		t.Fatalf("server that never answers must fail the download, got %s", obs.ObservedState)
	}
	if obs.LastError == nil || !strings.Contains(*obs.LastError, "no data for 200ms") {
		t.Errorf("error must name the silence, got %v", obs.LastError)
	}
}

type recordedReports struct {
	mu   sync.Mutex
	list []client.AgentContentObserved
}

func (r *recordedReports) record(_ context.Context, obs client.AgentContentObserved) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, obs)
}

func (r *recordedReports) last() client.AgentContentObserved {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.list) == 0 {
		return client.AgentContentObserved{}
	}
	return r.list[len(r.list)-1]
}

func TestReconcileReportsOutcomeOfDownloads(t *testing.T) {
	body := randomBody(4 << 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	t.Run("ready", func(t *testing.T) {
		reports := &recordedReports{}
		mgr := newTestManager()
		mgr.SetReporter(reports.record)
		obs := mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(t.TempDir()))
		if obs.ObservedState != client.ContentReady {
			t.Fatalf("want ready, got %s (err=%v)", obs.ObservedState, obs.LastError)
		}
		if got := reports.last(); got.ObservedState != client.ContentReady {
			t.Errorf("last report must be ready, got %s", got.ObservedState)
		}
	})

	t.Run("error", func(t *testing.T) {
		reports := &recordedReports{}
		mgr := newTestManager()
		mgr.SetReporter(reports.record)
		mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/missing", body), containerWith(t.TempDir()))
		if got := reports.last(); got.ObservedState != client.ContentError {
			t.Errorf("last report must be error, got %s", got.ObservedState)
		}
	})

	t.Run("nothing to download", func(t *testing.T) {
		reports := &recordedReports{}
		mgr := newTestManager()
		mgr.SetReporter(reports.record)
		root := t.TempDir()
		mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(root))
		reports.list = nil
		mgr.Reconcile(context.Background(), modelSpec(srv.URL+"/m", body), containerWith(root))
		if len(reports.list) != 0 {
			t.Errorf("reconcile with everything in place must not report, got %d reports", len(reports.list))
		}
	})
}

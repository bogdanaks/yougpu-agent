package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/container"
)

type fakeExec struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeExec) Run(_ context.Context, _ time.Duration, name string, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name+" "+strings.Join(args, " "))
	return "", nil
}

type recorder struct {
	mu   sync.Mutex
	list []client.AgentStateObserved
}

func (r *recorder) report(_ context.Context, obs client.AgentStateObserved) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, obs)
}

func (r *recorder) states() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, obs := range r.list {
		out = append(out, obs.ObservedState)
	}
	return out
}

func (r *recorder) count(state string) int {
	n := 0
	for _, s := range r.states() {
		if s == state {
			n++
		}
	}
	return n
}

func newManager(t *testing.T) (*Manager, *fakeExec, *recorder) {
	t.Helper()
	exec := &fakeExec{}
	rec := &recorder{}
	m := New(t.TempDir(), exec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.retryDelay = time.Millisecond
	m.restoringEvery = 10 * time.Millisecond
	m.saveWindow = time.Hour
	m.SetReporter(rec.report)
	return m, exec, rec
}

func ranHere(t *testing.T, m *Manager) {
	t.Helper()
	for _, name := range []string{restoredMarker, container.StartedMarker} {
		if err := os.WriteFile(m.marker(name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func (m *Manager) waitRestore() {
	m.mu.Lock()
	j := m.job
	m.mu.Unlock()
	if j != nil {
		<-j.done
	}
}

func restoreWithin(t *testing.T, m *Manager, spec *client.AgentStateSpec, root string) (bool, *client.AgentStateObserved) {
	t.Helper()
	m.Restore(context.Background(), spec, workspace(root))
	finished := make(chan struct{})
	go func() {
		m.waitRestore()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not finish")
	}
	return m.Restore(context.Background(), spec, workspace(root))
}

func workspace(root string) *client.AgentContainerSpec {
	return &client.AgentContainerSpec{Volumes: []client.ContainerVolume{{Host: root, Container: "/workspace"}}}
}

func archiveOf(t *testing.T, files map[string]string) ([]byte, string) {
	t.Helper()
	src := t.TempDir()
	for rel, body := range files {
		writeFile(t, src, rel, body, 0o644)
	}
	var buf bytes.Buffer
	if err := Pack(src, testInclude, testExclude, &buf, roomy); err != nil {
		t.Fatal(err)
	}
	return digest(buf.Bytes())
}

func digest(body []byte) ([]byte, string) {
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:])
}

func serve(body []byte) (*httptest.Server, *atomic.Int32) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write(body)
	}))
	return srv, &hits
}

func restoreSpec(url, sum string, size int) *client.AgentStateSpec {
	return &client.AgentStateSpec{Include: testInclude, Exclude: testExclude, Restore: &client.StateRestore{URL: url, SHA256: sum, SizeBytes: int64(size)}}
}

func TestNoStateStartsContainer(t *testing.T) {
	m, _, _ := newManager(t)
	ok, obs := m.Restore(context.Background(), nil, workspace(t.TempDir()))
	if !ok || obs != nil {
		t.Fatalf("ok=%v obs=%v", ok, obs)
	}
}

func TestRestoreWaitsWhileArchiveIsSaved(t *testing.T) {
	m, _, _ := newManager(t)
	ok, obs := m.Restore(context.Background(), &client.AgentStateSpec{Pending: true}, workspace(t.TempDir()))
	if ok || obs.ObservedState != client.StateWaiting {
		t.Fatalf("ok=%v state=%s", ok, obs.ObservedState)
	}
}

func TestCleanStartIsReportedRestored(t *testing.T) {
	m, _, _ := newManager(t)
	ok, obs := m.Restore(context.Background(), &client.AgentStateSpec{}, workspace(t.TempDir()))
	if !ok || obs.ObservedState != client.StateRestored {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
}

func TestRestoreUnpacksVerifiedArchiveOnce(t *testing.T) {
	body, sum := archiveOf(t, map[string]string{"custom_nodes/pack/__init__.py": "nodes", "user/default/tabs.json": "{}"})
	srv, hits := serve(body)
	defer srv.Close()
	m, _, rec := newManager(t)
	var notified atomic.Int32
	m.SetNotify(func() { notified.Add(1) })
	root := t.TempDir()
	spec := restoreSpec(srv.URL, sum, len(body))

	ok, obs := restoreWithin(t, m, spec, root)

	if !ok || obs.ObservedState != client.StateRestored {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
	if readFile(t, root, "custom_nodes/pack/__init__.py") != "nodes" {
		t.Fatal("not unpacked")
	}
	states := rec.states()
	if states[0] != client.StateRestoring || states[len(states)-1] != client.StateRestored {
		t.Fatalf("reported %v", states)
	}
	if notified.Load() != 1 {
		t.Fatalf("agent must be woken once the state is in place, notified %d", notified.Load())
	}
	if exists(root, restoreDir) {
		t.Fatal("temporary folder left behind")
	}

	ok, _ = m.Restore(context.Background(), spec, workspace(root))
	if !ok || hits.Load() != 1 {
		t.Fatalf("restored twice: ok=%v hits=%d", ok, hits.Load())
	}
}

func TestRestoreRunsInBackgroundAndKeepsReporting(t *testing.T) {
	body, sum := archiveOf(t, map[string]string{"user/default/tabs.json": "{}"})
	release := make(chan struct{})
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	m, _, rec := newManager(t)
	root := t.TempDir()
	spec := restoreSpec(srv.URL, sum, len(body))

	start := time.Now()
	ok, obs := m.Restore(context.Background(), spec, workspace(root))
	if ok || obs.ObservedState != client.StateRestoring || time.Since(start) > time.Second {
		t.Fatalf("Restore must return at once: ok=%v obs=%+v", ok, obs)
	}
	deadline := time.Now().Add(5 * time.Second)
	for rec.count(client.StateRestoring) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.count(client.StateRestoring) < 3 {
		t.Fatalf("restoring must be repeated while the archive downloads, got %v", rec.states())
	}
	if ok, obs := m.Restore(context.Background(), spec, workspace(root)); ok || obs.ObservedState != client.StateRestoring {
		t.Fatalf("second call while running: ok=%v obs=%+v", ok, obs)
	}
	close(release)
	m.waitRestore()
	if ok, _ := m.Restore(context.Background(), spec, workspace(root)); !ok || hits.Load() != 1 {
		t.Fatalf("ok=%v hits=%d", ok, hits.Load())
	}
}

func TestRestoreRejectsTamperedArchive(t *testing.T) {
	body, _ := archiveOf(t, map[string]string{"user/default/tabs.json": "{}"})
	srv, hits := serve(body)
	defer srv.Close()
	m, _, _ := newManager(t)
	root := t.TempDir()
	spec := restoreSpec(srv.URL, strings.Repeat("0", 64), len(body))

	ok, obs := restoreWithin(t, m, spec, root)

	if ok || obs.ObservedState != client.StateRestoreFailed || obs.LastError == nil {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
	if exists(root, "user") {
		t.Fatal("tampered archive unpacked")
	}
	if hits.Load() != attempts {
		t.Fatalf("failed restore must not start over on every call, hits=%d", hits.Load())
	}
}

func TestBrokenArchiveLeavesNoHalfWorkspace(t *testing.T) {
	body, sum := digest(hostile(t, regular("user/default/tabs.json", 2), regular("custom_nodes/pack/x.py", 2), regular("../escape", 1)).Bytes())
	srv, _ := serve(body)
	defer srv.Close()
	m, _, _ := newManager(t)
	root := t.TempDir()

	ok, obs := restoreWithin(t, m, restoreSpec(srv.URL, sum, len(body)), root)

	if ok || obs.ObservedState != client.StateRestoreFailed {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Fatalf("broken restore left %v in the workspace", entries)
	}
}

func TestRestoreMergesIntoWorkspace(t *testing.T) {
	body, sum := archiveOf(t, map[string]string{"user/default/tabs.json": "{}", "comfyui.db": "new"})
	srv, _ := serve(body)
	defer srv.Close()
	m, _, _ := newManager(t)
	root := t.TempDir()
	writeFile(t, root, "user/default/workflows/wf.json", "graph", 0o644)
	writeFile(t, root, "models/vae/m.bin", "weights", 0o644)
	writeFile(t, root, "comfyui.db", "old", 0o644)

	if ok, obs := restoreWithin(t, m, restoreSpec(srv.URL, sum, len(body)), root); !ok {
		t.Fatalf("obs=%+v", obs)
	}

	if readFile(t, root, "user/default/workflows/wf.json") != "graph" || readFile(t, root, "models/vae/m.bin") != "weights" {
		t.Fatal("files placed before the restore were lost")
	}
	if readFile(t, root, "user/default/tabs.json") != "{}" || readFile(t, root, "comfyui.db") != "new" {
		t.Fatal("archive not applied")
	}
}

func TestRestoreGivesUpAtDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	m.restoreTimeout = 100 * time.Millisecond

	ok, obs := restoreWithin(t, m, restoreSpec(srv.URL, strings.Repeat("0", 64), 10), t.TempDir())

	if ok || obs.ObservedState != client.StateRestoreFailed || obs.LastError == nil || !strings.Contains(*obs.LastError, "did not finish") {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
}

func TestRestoreDownloadFailsWhenServerGoesSilent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	m.idle = 100 * time.Millisecond

	ok, obs := restoreWithin(t, m, restoreSpec(srv.URL, strings.Repeat("0", 64), 1000), t.TempDir())

	if ok || obs.LastError == nil || !strings.Contains(*obs.LastError, "no data for 100ms") {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
}

func TestRestoreErrorsDoNotLeakSignedURL(t *testing.T) {
	var logs bytes.Buffer
	m := New(t.TempDir(), &fakeExec{}, slog.New(slog.NewTextHandler(&logs, nil)))
	m.retryDelay = time.Millisecond

	ok, obs := restoreWithin(t, m, restoreSpec("http://127.0.0.1:1/state?X-Amz-Signature=SECRET123", strings.Repeat("0", 64), 10), t.TempDir())

	if ok || obs.LastError == nil || strings.Contains(*obs.LastError, "SECRET123") {
		t.Fatalf("ok=%v err=%v", ok, obs.LastError)
	}
	if strings.Contains(logs.String(), "SECRET123") {
		t.Fatalf("log leaks the signature: %s", logs.String())
	}
}

func TestSaveStopsRunningRestoreQuietly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	m, _, rec := newManager(t)
	m.retryDelay = time.Hour
	root := t.TempDir()
	spec := restoreSpec(srv.URL, strings.Repeat("0", 64), 10)
	spec.Save = &client.StateSave{UploadURL: srv.URL}
	m.Restore(context.Background(), spec, workspace(root))

	done := make(chan *client.AgentStateObserved)
	go func() { done <- m.Save(context.Background(), spec, workspace(root), nil) }()
	var obs *client.AgentStateObserved
	select {
	case obs = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Save must stop the restore instead of waiting for it")
	}

	if obs.ObservedState != client.StateSaveSkipped {
		t.Fatalf("obs=%+v", obs)
	}
	if rec.count(client.StateRestoreFailed) != 0 {
		t.Fatalf("stopped restore must not be reported as failed: %v", rec.states())
	}
}

func TestFreezeRunsCommandInContainer(t *testing.T) {
	m, exec, rec := newManager(t)
	ranHere(t, m)
	spec := &client.AgentStateSpec{FreezeCommand: []string{"yougpu-freeze"}, Save: &client.StateSave{UploadURL: "http://x"}}

	m.Freeze(context.Background(), spec, "app_container")

	if len(exec.calls) != 1 || exec.calls[0] != "docker exec app_container yougpu-freeze" {
		t.Fatalf("calls %v", exec.calls)
	}
	if strings.Join(rec.states(), ",") != client.StateSaving {
		t.Fatalf("reported %v", rec.states())
	}
}

func TestFreezeSkippedWhenNothingWillBeSaved(t *testing.T) {
	m, exec, rec := newManager(t)
	spec := &client.AgentStateSpec{FreezeCommand: []string{"yougpu-freeze"}, Save: &client.StateSave{UploadURL: "http://x"}}

	m.Freeze(context.Background(), spec, "app_container")

	if len(exec.calls) != 0 || len(rec.states()) != 0 {
		t.Fatalf("calls %v reports %v", exec.calls, rec.states())
	}
}

func TestSaveUploadsArchiveOnce(t *testing.T) {
	var puts atomic.Int32
	var got []byte
	var length int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		puts.Add(1)
		length = r.ContentLength
		got, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()
	m, _, rec := newManager(t)
	ranHere(t, m)
	root := t.TempDir()
	writeFile(t, root, "user/default/tabs.json", "{}", 0o644)
	spec := &client.AgentStateSpec{Include: testInclude, Exclude: testExclude, Save: &client.StateSave{UploadURL: srv.URL}}

	obs := m.Save(context.Background(), spec, workspace(root), nil)

	sum := sha256.Sum256(got)
	if obs.ObservedState != client.StateSaved || obs.SHA256 == nil || *obs.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("obs=%+v", obs)
	}
	if obs.SizeBytes == nil || *obs.SizeBytes != int64(len(got)) || length != int64(len(got)) {
		t.Fatalf("size=%v length=%d body=%d", obs.SizeBytes, length, len(got))
	}
	if strings.Join(rec.states(), ",") != "saving,saved" {
		t.Fatalf("reported %v", rec.states())
	}

	again := m.Save(context.Background(), spec, workspace(root), nil)
	if puts.Load() != 1 || again.ObservedState != client.StateSaved || *again.SHA256 != *obs.SHA256 {
		t.Fatalf("saved twice: puts=%d obs=%+v", puts.Load(), again)
	}
	if out := m.Outcome(); out == nil || out.ObservedState != client.StateSaved || *out.SHA256 != *obs.SHA256 || *out.SizeBytes != *obs.SizeBytes {
		t.Fatalf("outcome=%+v", out)
	}
}

func TestSaveSkipsWorkspaceThatWasNeverRestored(t *testing.T) {
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	if err := os.WriteFile(m.marker(container.StartedMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeFile(t, root, "user/default/tabs.json", "{}", 0o644)
	spec := &client.AgentStateSpec{Include: testInclude, Exclude: testExclude, Pending: true, Save: &client.StateSave{UploadURL: srv.URL}}

	m.Restore(context.Background(), spec, workspace(root))
	obs := m.Save(context.Background(), spec, workspace(root), nil)

	if obs == nil || obs.ObservedState != client.StateSaveSkipped || puts.Load() != 0 {
		t.Fatalf("obs=%+v puts=%d", obs, puts.Load())
	}
	if out := m.Outcome(); out == nil || out.ObservedState != client.StateSaveSkipped {
		t.Fatalf("skip must be repeated from the marker, got %+v", out)
	}
}

func TestSaveSkipsWhenComfyNeverStartedHere(t *testing.T) {
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	root := t.TempDir()
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: srv.URL}}
	if ok, _ := m.Restore(context.Background(), spec, workspace(root)); !ok {
		t.Fatal("clean start must be restored")
	}

	obs := m.Save(context.Background(), spec, workspace(root), nil)

	if obs.ObservedState != client.StateSaveSkipped || puts.Load() != 0 {
		t.Fatalf("obs=%+v puts=%d", obs, puts.Load())
	}
}

func TestSaveRetriesUntilDeadline(t *testing.T) {
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	m, _, rec := newManager(t)
	ranHere(t, m)
	root := t.TempDir()
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: srv.URL}}

	obs := m.Save(context.Background(), spec, workspace(root), nil)
	if obs.ObservedState != client.StateSaving || obs.LastError == nil || !strings.Contains(*obs.LastError, "503") {
		t.Fatalf("attempt inside the window must stay saving, got %+v", obs)
	}
	if m.Outcome() != nil {
		t.Fatal("no outcome while retrying")
	}
	obs = m.Save(context.Background(), spec, workspace(root), nil)
	if obs.ObservedState != client.StateSaving || puts.Load() != 2*attempts {
		t.Fatalf("next tick must retry, obs=%+v puts=%d", obs, puts.Load())
	}

	past := strconv.FormatInt(time.Now().Add(-11*time.Minute).UnixMilli(), 10)
	if err := os.WriteFile(m.marker(saveStartMarker), []byte(past), 0o644); err != nil {
		t.Fatal(err)
	}
	m.saveWindow = saveWindow
	obs = m.Save(context.Background(), spec, workspace(root), nil)
	if obs.ObservedState != client.StateSaveFailed {
		t.Fatalf("after the deadline the save fails, got %+v", obs)
	}
	before := puts.Load()
	if again := m.Save(context.Background(), spec, workspace(root), nil); again.ObservedState != client.StateSaveFailed || puts.Load() != before {
		t.Fatalf("failed save must be repeated from the marker, got %+v puts=%d", again, puts.Load())
	}
	if rec.count(client.StateSaveFailed) != 1 {
		t.Fatalf("save_failed reported %d times: %v", rec.count(client.StateSaveFailed), rec.states())
	}
}

func TestSaveTooLargeFailsAtOnce(t *testing.T) {
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	m.maxUpload = 16
	ranHere(t, m)
	root := t.TempDir()
	writeFile(t, root, "user/default/tabs.json", strings.Repeat("{}", 1000), 0o644)
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: srv.URL}}

	obs := m.Save(context.Background(), spec, workspace(root), nil)

	if obs.ObservedState != client.StateSaveFailed || puts.Load() != 0 || !strings.Contains(*obs.LastError, "too large") {
		t.Fatalf("obs=%+v puts=%d", obs, puts.Load())
	}
}

func TestSaveDoesNotPackWhileContainerIsAlive(t *testing.T) {
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	ranHere(t, m)
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: srv.URL}}
	alive := errors.New("контейнер не остановился: abc")

	obs := m.Save(context.Background(), spec, workspace(t.TempDir()), alive)
	if obs.ObservedState != client.StateSaving || !strings.Contains(*obs.LastError, "контейнер не остановился") {
		t.Fatalf("obs=%+v", obs)
	}
	m.saveWindow = 0
	obs = m.Save(context.Background(), spec, workspace(t.TempDir()), alive)
	if obs.ObservedState != client.StateSaveFailed || puts.Load() != 0 {
		t.Fatalf("obs=%+v puts=%d", obs, puts.Load())
	}
}

func TestRejectedSaveReportsOnlyTheErrorCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code><Message>X-Amz-Credential=KEYID123</Message></Error>"))
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	m.saveWindow = 0
	ranHere(t, m)
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: srv.URL}}

	obs := m.Save(context.Background(), spec, workspace(t.TempDir()), nil)

	if obs.ObservedState != client.StateSaveFailed || obs.LastError == nil || *obs.LastError != "state upload: http 403 AccessDenied" {
		t.Fatalf("obs=%+v err=%v", obs, *obs.LastError)
	}
}

func TestSaveErrorsDoNotLeakSignedURL(t *testing.T) {
	var logs bytes.Buffer
	m := New(t.TempDir(), &fakeExec{}, slog.New(slog.NewTextHandler(&logs, nil)))
	m.retryDelay = time.Millisecond
	m.saveWindow = 0
	ranHere(t, m)
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: "http://127.0.0.1:1/state?X-Amz-Signature=SECRET123"}}

	obs := m.Save(context.Background(), spec, workspace(t.TempDir()), nil)

	if obs.ObservedState != client.StateSaveFailed || strings.Contains(*obs.LastError, "SECRET123") {
		t.Fatalf("obs=%+v err=%s", obs, *obs.LastError)
	}
	if strings.Contains(logs.String(), "SECRET123") {
		t.Fatalf("log leaks the signature: %s", logs.String())
	}
}

func TestNothingToSaveWithoutSaveSpec(t *testing.T) {
	m, _, rec := newManager(t)
	if obs := m.Save(context.Background(), &client.AgentStateSpec{Include: testInclude}, workspace(t.TempDir()), nil); obs != nil {
		t.Fatalf("obs=%+v", obs)
	}
	if len(rec.states()) != 0 {
		t.Fatalf("reported %v", rec.states())
	}
	if m.Outcome() != nil {
		t.Fatal("no outcome without a save")
	}
}

func TestRetryDelayFollowsContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	m, _, rec := newManager(t)
	m.retryDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	m.Restore(ctx, restoreSpec(srv.URL, strings.Repeat("0", 64), 10), workspace(t.TempDir()))
	time.Sleep(50 * time.Millisecond)

	cancel()
	finished := make(chan struct{})
	go func() {
		m.waitRestore()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("retry pause ignores cancellation")
	}
	if rec.count(client.StateRestoreFailed) != 0 {
		t.Fatalf("cancelled restore reported as failed: %v", rec.states())
	}
}

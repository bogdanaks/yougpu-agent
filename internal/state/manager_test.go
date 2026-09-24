package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
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
	mu     sync.Mutex
	states []string
}

func (r *recorder) report(_ context.Context, obs client.AgentStateObserved) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, obs.ObservedState)
}

func newManager(t *testing.T) (*Manager, *fakeExec, *recorder) {
	t.Helper()
	exec := &fakeExec{}
	rec := &recorder{}
	m := New(t.TempDir(), exec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.retryDelay = time.Millisecond
	m.SetReporter(rec.report)
	return m, exec, rec
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
	if err := Pack(src, testInclude, testExclude, &buf); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

func serve(body []byte) (*httptest.Server, *atomic.Int32) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write(body)
	}))
	return srv, &hits
}

func TestNoStateStartsContainer(t *testing.T) {
	m, _, _ := newManager(t)
	ok, obs := m.Restore(context.Background(), nil, workspace(t.TempDir()))
	if !ok || obs != nil {
		t.Fatalf("ok=%v obs=%v", ok, obs)
	}
}

func TestRestoreWaitsWhileArchiveIsSaved(t *testing.T) {
	m, _, rec := newManager(t)
	ok, obs := m.Restore(context.Background(), &client.AgentStateSpec{Pending: true}, workspace(t.TempDir()))
	if ok || obs.ObservedState != client.StateWaiting {
		t.Fatalf("ok=%v state=%s", ok, obs.ObservedState)
	}
	if len(rec.states) != 1 || rec.states[0] != client.StateWaiting {
		t.Fatalf("reported %v", rec.states)
	}
}

func TestRestoreUnpacksVerifiedArchiveOnce(t *testing.T) {
	body, sum := archiveOf(t, map[string]string{"custom_nodes/pack/__init__.py": "nodes", "user/default/tabs.json": "{}"})
	srv, hits := serve(body)
	defer srv.Close()
	m, _, rec := newManager(t)
	root := t.TempDir()
	spec := &client.AgentStateSpec{Restore: &client.StateRestore{URL: srv.URL, SHA256: sum, SizeBytes: int64(len(body))}}

	ok, obs := m.Restore(context.Background(), spec, workspace(root))

	if !ok || obs.ObservedState != client.StateRestored {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
	if readFile(t, root, "custom_nodes/pack/__init__.py") != "nodes" {
		t.Fatal("not unpacked")
	}
	if strings.Join(rec.states, ",") != "restoring,restored" {
		t.Fatalf("reported %v", rec.states)
	}

	ok, _ = m.Restore(context.Background(), spec, workspace(root))
	if !ok || hits.Load() != 1 {
		t.Fatalf("restored twice: ok=%v hits=%d", ok, hits.Load())
	}
}

func TestRestoreRejectsTamperedArchive(t *testing.T) {
	body, _ := archiveOf(t, map[string]string{"user/default/tabs.json": "{}"})
	srv, _ := serve(body)
	defer srv.Close()
	m, _, _ := newManager(t)
	root := t.TempDir()
	spec := &client.AgentStateSpec{Restore: &client.StateRestore{URL: srv.URL, SHA256: strings.Repeat("0", 64), SizeBytes: int64(len(body))}}

	ok, obs := m.Restore(context.Background(), spec, workspace(root))

	if ok || obs.ObservedState != client.StateRestoreFailed || obs.LastError == nil {
		t.Fatalf("ok=%v obs=%+v", ok, obs)
	}
	if exists(root, "user") {
		t.Fatal("tampered archive unpacked")
	}
}

func TestLateArchiveDoesNotOverwriteStartedWorkspace(t *testing.T) {
	body, sum := archiveOf(t, map[string]string{"user/default/tabs.json": "old"})
	srv, hits := serve(body)
	defer srv.Close()
	m, _, _ := newManager(t)
	root := t.TempDir()

	if ok, _ := m.Restore(context.Background(), &client.AgentStateSpec{}, workspace(root)); !ok {
		t.Fatal("clean start blocked")
	}
	spec := &client.AgentStateSpec{Restore: &client.StateRestore{URL: srv.URL, SHA256: sum, SizeBytes: int64(len(body))}}
	if ok, _ := m.Restore(context.Background(), spec, workspace(root)); !ok || hits.Load() != 0 {
		t.Fatalf("archive applied after start: ok=%v hits=%d", ok, hits.Load())
	}
}

func TestFreezeRunsCommandInContainer(t *testing.T) {
	m, exec, _ := newManager(t)
	spec := &client.AgentStateSpec{FreezeCommand: []string{"yougpu-freeze"}, Save: &client.StateSave{UploadURL: "http://x"}}

	m.Freeze(context.Background(), spec, "app_container")

	if len(exec.calls) != 1 || exec.calls[0] != "docker exec app_container yougpu-freeze" {
		t.Fatalf("calls %v", exec.calls)
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
	root := t.TempDir()
	writeFile(t, root, "user/default/tabs.json", "{}", 0o644)
	spec := &client.AgentStateSpec{Include: testInclude, Exclude: testExclude, Save: &client.StateSave{UploadURL: srv.URL}}

	obs := m.Save(context.Background(), spec, workspace(root))

	sum := sha256.Sum256(got)
	if obs.ObservedState != client.StateSaved || obs.SHA256 == nil || *obs.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("obs=%+v", obs)
	}
	if obs.SizeBytes == nil || *obs.SizeBytes != int64(len(got)) || length != int64(len(got)) {
		t.Fatalf("size=%v length=%d body=%d", obs.SizeBytes, length, len(got))
	}
	if strings.Join(rec.states, ",") != "saving,saved" {
		t.Fatalf("reported %v", rec.states)
	}

	again := m.Save(context.Background(), spec, workspace(root))
	if puts.Load() != 1 || again.ObservedState != client.StateSaved || *again.SHA256 != *obs.SHA256 {
		t.Fatalf("saved twice: puts=%d obs=%+v", puts.Load(), again)
	}
}

func TestRejectedSaveIsReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	m, _, _ := newManager(t)
	spec := &client.AgentStateSpec{Include: testInclude, Save: &client.StateSave{UploadURL: srv.URL}}

	obs := m.Save(context.Background(), spec, workspace(t.TempDir()))

	if obs.ObservedState != client.StateSaveFailed || obs.LastError == nil {
		t.Fatalf("obs=%+v", obs)
	}
}

func TestNothingToSaveWithoutSaveSpec(t *testing.T) {
	m, _, rec := newManager(t)
	if obs := m.Save(context.Background(), &client.AgentStateSpec{Include: testInclude}, workspace(t.TempDir())); obs != nil {
		t.Fatalf("obs=%+v", obs)
	}
	if len(rec.states) != 0 {
		t.Fatalf("reported %v", rec.states)
	}
}

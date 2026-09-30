package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/bogdanaks/yougpu-agent/internal/container"
)

const every = 600 * time.Second

type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

type put struct {
	key    string
	body   []byte
	length int64
}

type commit struct {
	Key       string `json:"key"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type urlRequest struct {
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type checkpointServer struct {
	srv        *httptest.Server
	mu         sync.Mutex
	urlCode    int
	commitCode int
	uploadURL  string
	hold       bool
	entered    chan struct{}
	urls       int
	asked      []urlRequest
	puts       []put
	commits    []commit
	saves      int
	tokens     map[string]bool
}

func newCheckpointServer(t *testing.T) *checkpointServer {
	t.Helper()
	s := &checkpointServer{entered: make(chan struct{}, 8), tokens: map[string]bool{}}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *checkpointServer) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/instances/i1/agent/state/checkpoint-url":
		var asked urlRequest
		_ = json.NewDecoder(r.Body).Decode(&asked)
		s.mu.Lock()
		s.urls++
		s.asked = append(s.asked, asked)
		s.tokens[r.Header.Get("x-provisioning-token")] = true
		key := "u1/checkpoints/" + strconv.Itoa(s.urls)
		code, upload := s.urlCode, s.uploadURL
		s.mu.Unlock()
		if code != 0 {
			w.WriteHeader(code)
			return
		}
		if upload == "" {
			upload = s.srv.URL + "/b2/" + key
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "code": 200, "data": map[string]string{"key": key, "upload_url": upload}})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/b2/"):
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		hold := s.hold
		s.mu.Unlock()
		if hold {
			s.entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		s.mu.Lock()
		s.puts = append(s.puts, put{key: strings.TrimPrefix(r.URL.Path, "/b2/"), body: body, length: r.ContentLength})
		s.mu.Unlock()
	case r.Method == http.MethodPost && r.URL.Path == "/instances/i1/agent/state/checkpoints":
		var c commit
		_ = json.NewDecoder(r.Body).Decode(&c)
		s.mu.Lock()
		s.commits = append(s.commits, c)
		s.tokens[r.Header.Get("x-provisioning-token")] = true
		code := s.commitCode
		s.mu.Unlock()
		if code == 0 {
			code = http.StatusNoContent
		}
		w.WriteHeader(code)
	case r.Method == http.MethodPut && r.URL.Path == "/save":
		_, _ = io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.saves++
		s.mu.Unlock()
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *checkpointServer) set(f func(*checkpointServer)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

func (s *checkpointServer) counts() (urls, puts, commits int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.urls, len(s.puts), len(s.commits)
}

func (s *checkpointServer) seen() (tokens map[string]bool, saves int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tokens = map[string]bool{}
	for k, v := range s.tokens {
		tokens[k] = v
	}
	return tokens, s.saves
}

func (s *checkpointServer) requests() []urlRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]urlRequest(nil), s.asked...)
}

func (s *checkpointServer) uploaded() ([]put, []commit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]put(nil), s.puts...), append([]commit(nil), s.commits...)
}

type harness struct {
	m     *Manager
	srv   *checkpointServer
	clock *clock
	root  string
	spec  *client.AgentStateSpec
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	m, _ := newManager(t)
	srv := newCheckpointServer(t)
	m.backend = client.New(srv.srv.URL+"/instances/i1/agent", "tok", "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	clk := &clock{at: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}
	m.now = clk.now
	t.Cleanup(func() {
		if j := m.currentCheckpoint(); j != nil {
			j.cancel()
			<-j.done
		}
	})
	ranHere(t, m)
	root := t.TempDir()
	writeFile(t, root, "user/default/tabs.json", "{}", 0o644)
	writeFile(t, root, "user/comfyui_8188.log", "log", 0o644)
	writeFile(t, root, "custom_nodes/pack/__init__.py", "nodes", 0o644)
	spec := &client.AgentStateSpec{
		Include:    testInclude,
		Exclude:    testExclude,
		Checkpoint: &client.StateCheckpoint{Include: []string{"user"}, EverySec: int64(every / time.Second), MaxBytes: 1 << 20},
	}
	return &harness{m: m, srv: srv, clock: clk, root: root, spec: spec}
}

func (m *Manager) currentCheckpoint() *checkpointJob {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkpoint
}

func waitCheckpoint(t *testing.T, m *Manager) {
	t.Helper()
	j := m.currentCheckpoint()
	if j == nil {
		return
	}
	select {
	case <-j.done:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not finish")
	}
}

func (h *harness) tick(t *testing.T, after time.Duration) {
	t.Helper()
	h.clock.add(after)
	h.m.Checkpoint(context.Background(), h.spec, workspace(h.root))
	waitCheckpoint(t, h.m)
}

func (h *harness) expect(t *testing.T, urls, puts, commits int) {
	t.Helper()
	u, p, c := h.srv.counts()
	if u != urls || p != puts || c != commits {
		t.Fatalf("urls=%d puts=%d commits=%d, want %d %d %d", u, p, c, urls, puts, commits)
	}
	if exists(h.m.stateDir, checkpointArchive) {
		t.Fatal("temporary checkpoint archive left behind")
	}
}

func TestCheckpointUploadsAndCommitsUserFolder(t *testing.T) {
	h := newHarness(t)

	h.tick(t, 0)
	h.expect(t, 0, 0, 0)
	h.tick(t, every)

	h.expect(t, 1, 1, 1)
	puts, commits := h.srv.uploaded()
	sum := sha256.Sum256(puts[0].body)
	want := commit{Key: "u1/checkpoints/1", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(puts[0].body))}
	if commits[0] != want || puts[0].key != want.Key || puts[0].length != want.SizeBytes {
		t.Fatalf("commit %+v, want %+v, put key %s length %d", commits[0], want, puts[0].key, puts[0].length)
	}
	if asked := h.srv.requests(); len(asked) != 1 || asked[0] != (urlRequest{SizeBytes: want.SizeBytes, SHA256: want.SHA256}) {
		t.Fatalf("upload url must be asked for the packed archive %d bytes %s, asked %+v", want.SizeBytes, want.SHA256, asked)
	}
	if tokens, _ := h.srv.seen(); !tokens["tok"] || len(tokens) != 1 {
		t.Fatalf("backend calls must carry the provisioning token, got %v", tokens)
	}
	dst := t.TempDir()
	if err := Unpack(bytes.NewReader(puts[0].body), dst, testInclude, roomy); err != nil {
		t.Fatal(err)
	}
	if readFile(t, dst, "user/default/tabs.json") != "{}" {
		t.Fatal("user folder not in the checkpoint")
	}
	if exists(dst, "custom_nodes") || exists(dst, "user/comfyui_8188.log") {
		t.Fatal("checkpoint must hold only its include without the excluded files")
	}
}

func TestCheckpointUploadsOnlyChanges(t *testing.T) {
	h := newHarness(t)
	h.tick(t, 0)
	h.tick(t, every)
	h.expect(t, 1, 1, 1)

	h.tick(t, every)
	h.expect(t, 1, 1, 1)

	writeFile(t, h.root, "user/default/workflows/new.json", "graph", 0o644)
	h.tick(t, every)
	h.expect(t, 2, 2, 2)
	_, commits := h.srv.uploaded()
	if commits[0].SHA256 == commits[1].SHA256 || commits[1].Key != "u1/checkpoints/2" {
		t.Fatalf("commits %+v", commits)
	}
}

func TestCheckpointRefusedByBackendUploadsNothing(t *testing.T) {
	h := newHarness(t)
	h.srv.set(func(s *checkpointServer) { s.urlCode = http.StatusConflict })

	h.tick(t, 0)
	h.tick(t, every)
	h.expect(t, 1, 0, 0)

	h.srv.set(func(s *checkpointServer) { s.urlCode = 0 })
	h.tick(t, every)
	h.expect(t, 2, 1, 1)
}

func TestCheckpointRefusedByBackendWaitsForTheNextInterval(t *testing.T) {
	var logs bytes.Buffer
	h := newHarness(t)
	h.m.log = slog.New(slog.NewTextHandler(&logs, nil))
	h.srv.set(func(s *checkpointServer) { s.urlCode = http.StatusBadRequest })

	h.tick(t, 0)
	h.tick(t, every)
	h.expect(t, 1, 0, 0)
	h.tick(t, every-time.Second)
	h.expect(t, 1, 0, 0)
	if !strings.Contains(logs.String(), "400") {
		t.Fatalf("refusal must be logged with its reason: %s", logs.String())
	}

	h.srv.set(func(s *checkpointServer) { s.urlCode = 0 })
	h.tick(t, time.Second)
	h.expect(t, 2, 1, 1)
}

func TestRejectedCommitIsNotRemembered(t *testing.T) {
	for _, code := range []int{http.StatusConflict, http.StatusBadRequest, http.StatusInternalServerError} {
		h := newHarness(t)
		h.srv.set(func(s *checkpointServer) { s.commitCode = code })

		h.tick(t, 0)
		h.tick(t, every)
		h.tick(t, every)

		h.expect(t, 2, 2, 2)
		_, commits := h.srv.uploaded()
		if commits[0].SHA256 != commits[1].SHA256 || commits[0].Key == commits[1].Key {
			t.Fatalf("http %d: unchanged folder must be sent again under a new key, got %+v", code, commits)
		}
	}
}

func TestCheckpointOverMaxBytesIsSkipped(t *testing.T) {
	h := newHarness(t)
	h.spec.Checkpoint.MaxBytes = 1000
	writeFile(t, h.root, "user/default/huge.json", string(randomBytes(8000)), 0o644)

	h.tick(t, 0)
	h.tick(t, every)
	h.expect(t, 0, 0, 0)

	if err := os.Remove(filepath.Join(h.root, "user/default/huge.json")); err != nil {
		t.Fatal(err)
	}
	h.tick(t, every)
	h.expect(t, 1, 1, 1)
}

func TestCheckpointWaitsForItsInterval(t *testing.T) {
	h := newHarness(t)
	h.tick(t, 0)
	h.tick(t, every-time.Second)
	h.expect(t, 0, 0, 0)
	h.tick(t, time.Second)
	h.expect(t, 1, 1, 1)

	h.tick(t, every)
	writeFile(t, h.root, "user/default/workflows/new.json", "graph", 0o644)
	h.tick(t, every-time.Second)
	h.expect(t, 1, 1, 1)
	h.tick(t, time.Second)
	h.expect(t, 2, 2, 2)
}

func TestCheckpointNeedsRestoredWorkspaceAndStartedComfy(t *testing.T) {
	for _, missing := range []string{restoredMarker, container.StartedMarker} {
		h := newHarness(t)
		if err := os.Remove(h.m.marker(missing)); err != nil {
			t.Fatal(err)
		}

		h.tick(t, 0)
		h.tick(t, every)
		h.tick(t, every)
		h.expect(t, 0, 0, 0)

		if err := os.WriteFile(h.m.marker(missing), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		h.tick(t, 0)
		h.expect(t, 0, 0, 0)
		h.tick(t, every)
		h.expect(t, 1, 1, 1)
	}
}

func TestNoCheckpointWithoutPermission(t *testing.T) {
	h := newHarness(t)
	h.spec.Checkpoint = nil

	h.tick(t, 0)
	h.tick(t, every)
	h.m.Checkpoint(context.Background(), nil, workspace(h.root))

	h.expect(t, 0, 0, 0)
}

func TestCheckpointStopsWithItsContext(t *testing.T) {
	h := newHarness(t)
	h.srv.set(func(s *checkpointServer) { s.hold = true })
	h.tick(t, 0)
	h.clock.add(every)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.m.Checkpoint(ctx, h.spec, workspace(h.root))
	select {
	case <-h.srv.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint upload did not start")
	}
	running := h.m.currentCheckpoint()
	h.clock.add(every)
	h.m.Checkpoint(ctx, h.spec, workspace(h.root))
	if h.m.currentCheckpoint() != running {
		t.Fatal("second checkpoint started while one is running")
	}

	cancel()
	waitCheckpoint(t, h.m)
	h.expect(t, 1, 0, 0)
}

func TestSaveWaitsForRunningCheckpoint(t *testing.T) {
	h := newHarness(t)
	h.srv.set(func(s *checkpointServer) { s.hold = true })
	h.spec.Save = &client.StateSave{UploadURL: h.srv.srv.URL + "/save"}
	h.tick(t, 0)
	h.clock.add(every)
	h.m.Checkpoint(context.Background(), h.spec, workspace(h.root))
	select {
	case <-h.srv.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint upload did not start")
	}
	running := h.m.currentCheckpoint()

	done := make(chan *client.AgentStateObserved)
	go func() { done <- h.m.Save(context.Background(), h.spec, workspace(h.root), nil) }()
	var obs *client.AgentStateObserved
	select {
	case obs = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Save must stop the running checkpoint instead of waiting for its upload")
	}
	select {
	case <-running.done:
	default:
		t.Fatal("Save went on while the checkpoint was still running")
	}
	if _, saves := h.srv.seen(); obs.ObservedState != client.StateSaved || saves != 1 {
		t.Fatalf("obs=%+v saves=%d", obs, saves)
	}
	h.expect(t, 1, 0, 0)

	h.srv.set(func(s *checkpointServer) { s.hold = false })
	h.tick(t, every)
	h.tick(t, every)
	if h.m.currentCheckpoint() != running {
		t.Fatal("checkpoint started after the final save began")
	}
	h.expect(t, 1, 0, 0)
}

func TestCheckpointErrorsDoNotLeakSignedURL(t *testing.T) {
	var logs bytes.Buffer
	h := newHarness(t)
	h.m.log = slog.New(slog.NewTextHandler(&logs, nil))
	h.srv.set(func(s *checkpointServer) { s.uploadURL = "http://127.0.0.1:1/b2?X-Amz-Signature=SECRET123" })

	h.tick(t, 0)
	h.tick(t, every)

	h.expect(t, 1, 0, 0)
	if !strings.Contains(logs.String(), "checkpoint") {
		t.Fatalf("failed checkpoint must be logged: %s", logs.String())
	}
	if strings.Contains(logs.String(), "SECRET123") {
		t.Fatalf("log leaks the signature: %s", logs.String())
	}
}

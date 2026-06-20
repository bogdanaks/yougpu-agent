package content

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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

func TestReconcileNoWorkspaceVolume(t *testing.T) {
	spec := &client.AgentContentSpec{Models: []client.ContentModel{{URL: "http://x/m", Type: "vae", Name: "m"}}}
	obs := newTestManager().Reconcile(context.Background(), spec, &client.AgentContainerSpec{})
	if obs.ObservedState != client.ContentError {
		t.Fatalf("want error without /workspace volume, got %s", obs.ObservedState)
	}
}

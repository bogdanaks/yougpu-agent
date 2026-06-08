package container

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func ptrStr(s string) *string   { return &s }
func ptrF64(f float64) *float64 { return &f }

func sampleSpec() *client.AgentContainerSpec {
	return &client.AgentContainerSpec{
		Image:      "pytorch/pytorch:latest",
		RunCommand: nil,
		Env:        map[string]string{"B": "2", "A": "1"},
		Volumes:    []client.ContainerVolume{{Host: "/data", Container: "/workspace"}},
		ShmSizeGB:  ptrF64(8),
		GPU:        true,
	}
}

func TestSpecHashStableRegardlessOfEnvOrder(t *testing.T) {
	a := sampleSpec()
	b := sampleSpec()
	b.Env = map[string]string{"A": "1", "B": "2"}
	if SpecHash(a) != SpecHash(b) {
		t.Fatalf("hash must be independent of map literal order")
	}
}

func TestSpecHashChangesOnContentChange(t *testing.T) {
	a := sampleSpec()
	b := sampleSpec()
	b.Image = "pytorch/pytorch:2.0"
	if SpecHash(a) == SpecHash(b) {
		t.Fatalf("hash must change when image changes")
	}
}

func TestSpecHashNil(t *testing.T) {
	if SpecHash(nil) != "" {
		t.Fatalf("nil spec hash must be empty")
	}
}

func TestDecide(t *testing.T) {
	hash := SpecHash(sampleSpec())
	cases := []struct {
		name    string
		hasSpec bool
		obs     Observed
		want    Action
	}{
		{"no spec, no container", false, Observed{}, ActionNone},
		{"no spec, orphan container", false, Observed{Exists: true, Running: true}, ActionRemove},
		{"spec, missing", true, Observed{}, ActionApply},
		{"spec, running matching hash", true, Observed{Exists: true, Running: true, SpecHash: hash}, ActionNone},
		{"spec, running stale hash", true, Observed{Exists: true, Running: true, SpecHash: "old"}, ActionApply},
		{"spec, exists but stopped", true, Observed{Exists: true, Running: false, SpecHash: hash}, ActionApply},
	}
	for _, c := range cases {
		if got := Decide(c.hasSpec, hash, c.obs); got != c.want {
			t.Errorf("%s: Decide=%v want %v", c.name, got, c.want)
		}
	}
}

func TestRunArgsContainsCoreFlags(t *testing.T) {
	m := NewManager(nil, testLogger())
	args := m.runArgs(sampleSpec(), "deadbeef")
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"run -d", "--name app_container", "--restart unless-stopped", "--network host",
		"--label yougpu.managed=true", "--label yougpu.spec.hash=deadbeef",
		"--gpus all", "--shm-size=8g", "-v /data:/workspace:rw",
		"-e A=1", "-e B=2", "pytorch/pytorch:latest",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("runArgs missing %q in: %s", want, joined)
		}
	}
}

func TestRunArgsRunCommandAppended(t *testing.T) {
	m := NewManager(nil, testLogger())
	spec := sampleSpec()
	spec.RunCommand = ptrStr("python main.py")
	args := m.runArgs(spec, "h")
	joined := strings.Join(args, " ")
	if !strings.HasSuffix(joined, "pytorch/pytorch:latest sh -c python main.py") {
		t.Errorf("run command must follow image: %s", joined)
	}
}

func TestRunArgsNoShmFallsBackToIpcHost(t *testing.T) {
	m := NewManager(nil, testLogger())
	spec := sampleSpec()
	spec.ShmSizeGB = nil
	joined := strings.Join(m.runArgs(spec, "h"), " ")
	if !strings.Contains(joined, "--ipc=host") || strings.Contains(joined, "--shm-size") {
		t.Errorf("expected --ipc=host fallback, got: %s", joined)
	}
}

type scriptExec struct {
	t        *testing.T
	inspect  string
	inspErr  error
	runCalls *[]string
}

func (s scriptExec) Run(_ context.Context, _ time.Duration, name string, args ...string) (string, error) {
	if name == "docker" && len(args) > 0 && args[0] == "inspect" {
		return s.inspect, s.inspErr
	}
	if s.runCalls != nil {
		*s.runCalls = append(*s.runCalls, name+" "+strings.Join(args, " "))
	}
	return "", nil
}

func TestReconcileNoopWhenRunningMatches(t *testing.T) {
	spec := sampleSpec()
	hash := SpecHash(spec)
	var calls []string
	m := NewManager(scriptExec{t: t, inspect: "true|" + hash, runCalls: &calls}, testLogger())

	obs := m.Reconcile(context.Background(), spec)
	if obs.ObservedState != client.ContainerRunning {
		t.Fatalf("want running, got %s", obs.ObservedState)
	}
	for _, c := range calls {
		if strings.Contains(c, "docker pull") || strings.Contains(c, "docker run") {
			t.Fatalf("must not recreate matching container, but called: %s", c)
		}
	}
}

func TestReconcileAppliesWhenAbsent(t *testing.T) {
	spec := sampleSpec()
	spec.Volumes = nil
	var calls []string
	m := NewManager(scriptExec{t: t, inspect: "", inspErr: fmt.Errorf("Error: No such object: app_container"), runCalls: &calls}, testLogger())

	_ = m.Reconcile(context.Background(), spec)
	joined := strings.Join(calls, " || ")
	if !strings.Contains(joined, "docker pull pytorch/pytorch:latest") || !strings.Contains(joined, "docker run -d") {
		t.Fatalf("expected pull+run on absent container, calls: %s", joined)
	}
}

func TestReconcileRemovesOrphanWhenNoSpec(t *testing.T) {
	var calls []string
	m := NewManager(scriptExec{t: t, inspect: "true|", runCalls: &calls}, testLogger())

	obs := m.Reconcile(context.Background(), nil)
	if obs.ObservedState != client.ContainerAbsent {
		t.Fatalf("want absent after removing orphan, got %s", obs.ObservedState)
	}
	if !strings.Contains(strings.Join(calls, " "), "docker rm -f app_container") {
		t.Fatalf("expected rm -f, calls: %v", calls)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

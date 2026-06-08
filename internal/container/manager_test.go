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
	m := NewManager(nil, nil, testLogger())
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
	m := NewManager(nil, nil, testLogger())
	spec := sampleSpec()
	spec.RunCommand = ptrStr("python main.py")
	args := m.runArgs(spec, "h")
	joined := strings.Join(args, " ")
	if !strings.HasSuffix(joined, "pytorch/pytorch:latest sh -c python main.py") {
		t.Errorf("run command must follow image: %s", joined)
	}
}

func TestRunArgsNoShmFallsBackToIpcHost(t *testing.T) {
	m := NewManager(nil, nil, testLogger())
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

type fakePuller struct {
	pulled   []string
	progress []PullProgress
	err      error
}

func (p *fakePuller) Pull(_ context.Context, image string, onProgress func(PullProgress)) error {
	p.pulled = append(p.pulled, image)
	for _, pp := range p.progress {
		if onProgress != nil {
			onProgress(pp)
		}
	}
	return p.err
}

func TestReconcileNoopWhenRunningMatches(t *testing.T) {
	spec := sampleSpec()
	hash := SpecHash(spec)
	var calls []string
	puller := &fakePuller{}
	m := NewManager(scriptExec{t: t, inspect: "true|" + hash, runCalls: &calls}, puller, testLogger())

	obs := m.Reconcile(context.Background(), spec)
	if obs.ObservedState != client.ContainerRunning {
		t.Fatalf("want running, got %s", obs.ObservedState)
	}
	if len(puller.pulled) != 0 {
		t.Fatalf("must not pull matching container, pulled: %v", puller.pulled)
	}
	for _, c := range calls {
		if strings.Contains(c, "docker run") {
			t.Fatalf("must not recreate matching container, but called: %s", c)
		}
	}
}

func TestReconcileAppliesWhenAbsent(t *testing.T) {
	spec := sampleSpec()
	spec.Volumes = nil
	var calls []string
	puller := &fakePuller{}
	m := NewManager(scriptExec{t: t, inspect: "", inspErr: fmt.Errorf("Error: No such object: app_container"), runCalls: &calls}, puller, testLogger())

	_ = m.Reconcile(context.Background(), spec)
	if len(puller.pulled) != 1 || puller.pulled[0] != "pytorch/pytorch:latest" {
		t.Fatalf("expected pull of image, pulled: %v", puller.pulled)
	}
	if !strings.Contains(strings.Join(calls, " || "), "docker run -d") {
		t.Fatalf("expected run on absent container, calls: %v", calls)
	}
}

func TestReconcileEmitsPhases(t *testing.T) {
	spec := sampleSpec()
	spec.Volumes = nil
	hash := SpecHash(spec)
	var calls []string
	puller := &fakePuller{progress: []PullProgress{{Percent: 50, LayersDone: 1, LayersTotal: 2}}}
	m := NewManager(scriptExec{t: t, inspect: "", inspErr: fmt.Errorf("No such object"), runCalls: &calls}, puller, testLogger())

	var phases []string
	m.SetReporter(func(_ context.Context, obs client.AgentContainerObserved) {
		phases = append(phases, obs.ObservedState)
		if obs.ObservedState == client.ContainerPulling && obs.Progress != nil && *obs.Progress == 50 {
			if obs.Detail == nil || *obs.Detail != "1/2" {
				t.Errorf("expected detail 1/2, got %v", obs.Detail)
			}
			if obs.SpecHash != hash {
				t.Errorf("expected spec hash %s, got %s", hash, obs.SpecHash)
			}
		}
	})

	_ = m.Reconcile(context.Background(), spec)
	joined := strings.Join(phases, ",")
	if !strings.HasPrefix(joined, "pulling,") {
		t.Fatalf("first phase must be pulling, got: %s", joined)
	}
	if !strings.Contains(joined, "starting") {
		t.Fatalf("expected starting phase before run, got: %s", joined)
	}
	pullingIdx := strings.Index(joined, "pulling")
	startingIdx := strings.Index(joined, "starting")
	if startingIdx < pullingIdx {
		t.Fatalf("starting must come after pulling, got: %s", joined)
	}
}

func TestReconcileRemovesOrphanWhenNoSpec(t *testing.T) {
	var calls []string
	m := NewManager(scriptExec{t: t, inspect: "true|", runCalls: &calls}, nil, testLogger())

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

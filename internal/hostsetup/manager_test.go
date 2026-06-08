package hostsetup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeExec struct {
	calls *[]string

	present       map[string]bool
	gpu           bool
	dockerUp      bool
	rcloneOK      bool
	fuseOK        bool
	aptLockFree   bool
	nvidiaRuntime []bool
	nvidiaProbes  int
	failContains  string
}

func (f *fakeExec) Run(_ context.Context, _ time.Duration, name string, args ...string) (string, error) {
	full := name + " " + strings.Join(args, " ")
	if f.calls != nil {
		*f.calls = append(*f.calls, full)
	}

	script := ""
	if name == "sh" && len(args) >= 2 && args[0] == "-c" {
		script = args[1]
	}

	switch {
	case strings.HasPrefix(script, "command -v "):
		cmd := strings.TrimSpace(strings.TrimPrefix(script, "command -v "))
		if f.present[cmd] {
			return "/usr/bin/" + cmd, nil
		}
		return "", errors.New("not found")

	case strings.Contains(script, "lspci") && strings.Contains(script, "nvidia"):
		if f.gpu {
			return "01:00.0 NVIDIA Corporation", nil
		}
		return "", errors.New("no nvidia on pci bus")

	case strings.Contains(script, "user_allow_other") && !strings.Contains(script, "echo"):
		if f.fuseOK {
			return "", nil
		}
		return "", errors.New("not configured")

	case strings.Contains(script, "fuser"):
		if f.aptLockFree {
			return "", errors.New("no lock holder")
		}
		return "", nil

	case name == "docker" && len(args) >= 2 && args[0] == "info" && args[1] == "--format":
		idx := f.nvidiaProbes
		f.nvidiaProbes++
		has := false
		if len(f.nvidiaRuntime) > 0 {
			if idx >= len(f.nvidiaRuntime) {
				idx = len(f.nvidiaRuntime) - 1
			}
			has = f.nvidiaRuntime[idx]
		}
		if has {
			return "map[nvidia:... runc:...]", nil
		}
		return "map[runc:...]", nil

	case name == "docker" && len(args) >= 1 && args[0] == "info":
		if f.dockerUp {
			return "Server Version: 27.0", nil
		}
		return "", errors.New("cannot connect to docker daemon")

	case name == "rclone" && len(args) >= 1 && args[0] == "version":
		if f.rcloneOK {
			return "rclone v1.66", nil
		}
		return "", errors.New("rclone not installed")
	}

	if f.failContains != "" && script != "" && strings.Contains(script, f.failContains) {
		return "partial stdout before failure", errors.New("exit status 1 (stderr: boom)")
	}
	return "", nil
}

func newManager(fe *fakeExec) *Manager {
	m := NewManager(fe, system.NewSystemd(fe, testLogger()), testLogger())
	m.SetWaitsForTest(0, 5, 3)
	return m
}

func joined(calls []string) string { return strings.Join(calls, " | ") }

func TestReconcileReadyHostIsNoop(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:         &calls,
		present:       map[string]bool{"gpg": true, "unzip": true, "lspci": true, "nvidia-ctk": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
		nvidiaRuntime: []bool{true},
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("ready host → ready, got %s", obs.ObservedState)
	}
	for _, c := range calls {
		if strings.Contains(c, "apt-get install") || strings.Contains(c, "get.docker.com") ||
			strings.Contains(c, "nvidia-ctk runtime configure") || strings.Contains(c, "rclone-current") {
			t.Errorf("ready host must not mutate, got call: %s", c)
		}
	}
}

func TestReconcileFreshHostRunsAllStepsInOrder(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:         &calls,
		present:       map[string]bool{},
		gpu:           true,
		dockerUp:      false,
		rcloneOK:      false,
		fuseOK:        false,
		aptLockFree:   true,
		nvidiaRuntime: []bool{true},
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("fresh host → ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	dockerAt := strings.Index(j, "get.docker.com")
	nvidiaAt := strings.Index(j, "nvidia-ctk runtime configure")
	rcloneAt := strings.Index(j, "rclone-current")
	if dockerAt < 0 || nvidiaAt < 0 || rcloneAt < 0 {
		t.Fatalf("all steps must run, calls: %s", j)
	}
	if !(dockerAt < nvidiaAt && nvidiaAt < rcloneAt) {
		t.Errorf("order must be docker→nvidia→rclone, calls: %s", j)
	}
}

func TestReconcileSkipsNvidiaWhenNoGPU(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:       &calls,
		present:     map[string]bool{"gpg": true, "unzip": true, "lspci": true},
		gpu:         false,
		dockerUp:    true,
		rcloneOK:    true,
		fuseOK:      true,
		aptLockFree: true,
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("expected ready, got %s", obs.ObservedState)
	}
	j := joined(calls)
	if strings.Contains(j, "nvidia-ctk") || strings.Contains(j, "nvidia-container-toolkit") {
		t.Errorf("no GPU → zero nvidia mutations, calls: %s", j)
	}
}

func TestReconcileDockerHardResetAndRuntimeWait(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:         &calls,
		present:       map[string]bool{"gpg": true, "unzip": true, "lspci": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
		aptLockFree:   true,
		nvidiaRuntime: []bool{false, false, true},
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("runtime appears → ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	if !strings.Contains(j, "systemctl stop docker") || !strings.Contains(j, "systemctl start docker") {
		t.Errorf("nvidia config must hard-reset docker (stop→start), calls: %s", j)
	}
}

func TestReconcileNvidiaRuntimeNeverAppearsIsError(t *testing.T) {
	fe := &fakeExec{
		present:       map[string]bool{"gpg": true, "unzip": true, "lspci": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
		aptLockFree:   true,
		nvidiaRuntime: []bool{false},
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupError {
		t.Fatalf("runtime never appears must be error (no false-green GPU), got %s", obs.ObservedState)
	}
	if obs.Detail == nil || *obs.Detail != client.SetupConfiguringGPU {
		t.Errorf("error must carry the failing phase, got %v", obs.Detail)
	}
}

func TestReconcileStopsOnFirstErrorAndReportsBundle(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:        &calls,
		present:      map[string]bool{"lspci": true},
		dockerUp:     true,
		failContains: "apt-get install -y gnupg",
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupError {
		t.Fatalf("failed step → error, got %s", obs.ObservedState)
	}
	if obs.LastError == nil || *obs.LastError == "" {
		t.Error("error must carry last_error")
	}
	if obs.LastLog == nil || !strings.Contains(*obs.LastLog, "partial stdout before failure") {
		t.Errorf("last_log bundle must include failing command output, got %v", obs.LastLog)
	}
	if strings.Contains(joined(calls), "get.docker.com") {
		t.Errorf("steps after a failure must NOT run, calls: %s", joined(calls))
	}
}

func TestReconcileEmitsPhasesInOrder(t *testing.T) {
	var emits []string
	fe := &fakeExec{
		present:       map[string]bool{},
		gpu:           true,
		dockerUp:      false,
		rcloneOK:      false,
		fuseOK:        false,
		aptLockFree:   true,
		nvidiaRuntime: []bool{true},
	}
	m := newManager(fe)
	m.SetReporter(func(_ context.Context, obs client.AgentSetupObserved) {
		emits = append(emits, obs.ObservedState)
	})
	final := m.Reconcile(context.Background())

	want := []string{
		client.SetupInstallingBase,
		client.SetupInstallingDocker,
		client.SetupConfiguringGPU,
		client.SetupInstallingStorage,
	}
	if len(emits) != len(want) {
		t.Fatalf("expected %d phase emits, got %d: %v", len(want), len(emits), emits)
	}
	for i := range want {
		if emits[i] != want[i] {
			t.Errorf("emit %d: want %s got %s", i, want[i], emits[i])
		}
	}
	if final.ObservedState != client.SetupReady || final.Progress == nil || *final.Progress != 100 {
		t.Errorf("final must be ready at 100%%, got %s %v", final.ObservedState, final.Progress)
	}
}

func TestReconcileProgressMonotonic(t *testing.T) {
	var progresses []int
	fe := &fakeExec{
		present:       map[string]bool{},
		gpu:           true,
		dockerUp:      false,
		rcloneOK:      false,
		fuseOK:        false,
		aptLockFree:   true,
		nvidiaRuntime: []bool{true},
	}
	m := newManager(fe)
	m.SetReporter(func(_ context.Context, obs client.AgentSetupObserved) {
		if obs.Progress != nil {
			progresses = append(progresses, *obs.Progress)
		}
	})
	m.Reconcile(context.Background())
	for i := 1; i < len(progresses); i++ {
		if progresses[i] < progresses[i-1] {
			t.Errorf("progress must be monotonic, got %v", progresses)
		}
	}
}

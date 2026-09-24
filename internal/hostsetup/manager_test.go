package hostsetup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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

	present        map[string]bool
	gpu            bool
	dockerUp       bool
	rcloneOK       bool
	fuseOK         bool
	aptConfPresent bool
	nvidiaRuntime  []bool
	nvidiaProbes   int
	failContains   string
	failTimes      int
	failSeen       int
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

	case strings.HasPrefix(script, "test -f /etc/apt/apt.conf.d/"):
		if f.aptConfPresent {
			return "", nil
		}
		return "", errors.New("no such file")

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
		if f.failTimes == 0 || f.failSeen < f.failTimes {
			f.failSeen++
			return "partial stdout before failure", errors.New("exit status 1 (stderr: boom)")
		}
	}
	return "", nil
}

func newManager(t *testing.T, fe *fakeExec) *Manager {
	t.Helper()
	m := NewManager(fe, system.NewSystemd(fe, testLogger()), testLogger())
	m.SetWaitsForTest(0, 5, 3)
	withDockerConfig(t, m, `{"max-concurrent-downloads": 8}`)
	return m
}

func withDockerConfig(t *testing.T, m *Manager, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "daemon.json")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	m.SetDockerConfigForTest(path)
	return path
}

func readDockerConfig(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read docker config: %v", err)
	}
	cfg := map[string]any{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse docker config: %v (%s)", err, data)
	}
	return cfg
}

func readyHost(calls *[]string) *fakeExec {
	return &fakeExec{
		calls:          calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
	}
}

func TestReconcileRaisesDockerPullConcurrencyKeepingOtherSettings(t *testing.T) {
	var calls []string
	m := newManager(t, readyHost(&calls))
	path := withDockerConfig(t, m, `{"runtimes": {"nvidia": {"path": "nvidia-container-runtime", "args": []}}, "default-runtime": "nvidia"}`)

	obs := m.Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("expected ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	cfg := readDockerConfig(t, path)
	if cfg["max-concurrent-downloads"] != float64(8) {
		t.Errorf("max-concurrent-downloads must be 8, got %v", cfg["max-concurrent-downloads"])
	}
	if cfg["default-runtime"] != "nvidia" || cfg["runtimes"] == nil {
		t.Errorf("existing settings must survive, got %v", cfg)
	}
	j := joined(calls)
	if !strings.Contains(j, "systemctl stop docker") || !strings.Contains(j, "systemctl start docker") {
		t.Errorf("docker must be restarted to apply the setting, calls: %s", j)
	}
	if strings.Contains(j, "get.docker.com") {
		t.Errorf("running docker must not be reinstalled, calls: %s", j)
	}
}

func TestReconcileCreatesDockerConfigWhenMissing(t *testing.T) {
	var calls []string
	m := newManager(t, readyHost(&calls))
	path := withDockerConfig(t, m, "")

	if obs := m.Reconcile(context.Background()); obs.ObservedState != client.SetupReady {
		t.Fatalf("expected ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	if cfg := readDockerConfig(t, path); cfg["max-concurrent-downloads"] != float64(8) {
		t.Errorf("max-concurrent-downloads must be 8, got %v", cfg)
	}
}

func TestReconcileKeepsTunedDockerRunning(t *testing.T) {
	var calls []string
	m := newManager(t, readyHost(&calls))
	withDockerConfig(t, m, `{"max-concurrent-downloads": 16}`)

	if obs := m.Reconcile(context.Background()); obs.ObservedState != client.SetupReady {
		t.Fatalf("expected ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	if j := joined(calls); strings.Contains(j, "systemctl stop docker") {
		t.Errorf("tuned docker must not be restarted, calls: %s", j)
	}
}

func joined(calls []string) string { return strings.Join(calls, " | ") }

func TestReconcileReadyHostIsNoop(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
	}
	obs := newManager(t, fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("ready host → ready, got %s", obs.ObservedState)
	}
	for _, c := range calls {
		if strings.Contains(c, "apt-get -o DPkg::Lock::Timeout") || strings.Contains(c, "get.docker.com") ||
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
		nvidiaRuntime: []bool{true},
	}
	obs := newManager(t, fe).Reconcile(context.Background())
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
		calls:    &calls,
		present:  map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:      false,
		dockerUp: true,
		rcloneOK: true,
		fuseOK:   true,
	}
	obs := newManager(t, fe).Reconcile(context.Background())
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
		present:       map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
		nvidiaRuntime: []bool{false, false, true},
	}
	obs := newManager(t, fe).Reconcile(context.Background())
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
		present:       map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
		nvidiaRuntime: []bool{false},
	}
	obs := newManager(t, fe).Reconcile(context.Background())
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
		failContains: "install -y gnupg",
	}
	obs := newManager(t, fe).Reconcile(context.Background())
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

func TestAptCallsAlwaysWaitForDpkgLock(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:         &calls,
		present:       map[string]bool{},
		gpu:           true,
		dockerUp:      false,
		rcloneOK:      false,
		fuseOK:        false,
		nvidiaRuntime: []bool{true},
	}
	newManager(t, fe).Reconcile(context.Background())

	seen := 0
	for _, c := range calls {
		if !strings.Contains(c, "apt-get") {
			continue
		}
		seen++
		if !strings.Contains(c, "-o DPkg::Lock::Timeout=300") {
			t.Errorf("apt-get without lock wait (unattended-upgrades would fail it instantly): %s", c)
		}
	}
	if seen == 0 {
		t.Fatal("expected apt-get calls on a fresh host")
	}
}

func TestAptRetriesTransientFailure(t *testing.T) {
	fe := &fakeExec{
		present:       map[string]bool{},
		gpu:           true,
		dockerUp:      false,
		rcloneOK:      false,
		fuseOK:        false,
		nvidiaRuntime: []bool{true},
		failContains:  "install -y gnupg",
		failTimes:     2,
	}
	obs := newManager(t, fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("apt must survive %d transient failures, got %s (err %v)", fe.failTimes, obs.ObservedState, obs.LastError)
	}
}

func TestAptGivesUpAfterAllTries(t *testing.T) {
	fe := &fakeExec{
		present:      map[string]bool{},
		dockerUp:     true,
		failContains: "install -y gnupg",
		failTimes:    3,
	}
	obs := newManager(t, fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupError {
		t.Fatalf("failures beyond the retry budget → error, got %s", obs.ObservedState)
	}
}

func TestReconcileStockImageNeedsNoApt(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "fusermount3": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       false,
		fuseOK:         false,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
	}
	obs := newManager(t, fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("stock image → ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	if strings.Contains(j, "apt-get") {
		t.Errorf("stock image must not touch apt at all, calls: %s", j)
	}
	if !strings.Contains(j, "rclone-current") || !strings.Contains(j, "zipfile") {
		t.Errorf("rclone must be fetched and unpacked without unzip, calls: %s", j)
	}
}

func TestReconcileFallsBackToUnzipWithoutPython(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "fusermount3": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       false,
		fuseOK:         false,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
		failContains:   "zipfile",
	}
	obs := newManager(t, fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("missing python3 → still ready via unzip, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	if !strings.Contains(j, "install -y unzip") || !strings.Contains(j, "unzip -q -o /tmp/rclone.zip") {
		t.Errorf("must install and use unzip when python3 is unavailable, calls: %s", j)
	}
}

func TestReconcileInstallsFuseOnlyWhenMissing(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         false,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
	}
	obs := newManager(t, fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("expected ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	if !strings.Contains(joined(calls), "install -y fuse3") {
		t.Errorf("absent fusermount must trigger fuse3 install, calls: %s", joined(calls))
	}
}

func TestReconcileWritesAptLockConfigOnceWhenMissing(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:            false,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: false,
	}
	newManager(t, fe).Reconcile(context.Background())

	writes := 0
	for _, c := range calls {
		if strings.Contains(c, "printf") && strings.Contains(c, "DPkg::Lock::Timeout") {
			writes++
		}
	}
	if writes != 1 {
		t.Errorf("missing config must be written exactly once, got %d writes: %s", writes, joined(calls))
	}
}

func TestReconcileKeepsExistingAptLockConfig(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:            false,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: true,
	}
	newManager(t, fe).Reconcile(context.Background())

	if strings.Contains(joined(calls), "printf") {
		t.Errorf("existing config must not be rewritten (cloud-init owns it), calls: %s", joined(calls))
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
		nvidiaRuntime: []bool{true},
	}
	m := newManager(t, fe)
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
		nvidiaRuntime: []bool{true},
	}
	m := newManager(t, fe)
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

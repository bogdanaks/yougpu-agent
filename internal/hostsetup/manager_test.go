package hostsetup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

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
	rcloneOld      bool
	fuseOK         bool
	aptConfPresent bool
	nvidiaRuntime  []bool
	nvidiaProbes   int
	failContains   string
	failTimes      int
	failSeen       int
	failOutput     string
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

	case (name == "rclone" || name == "/usr/bin/rclone") && len(args) >= 1 && args[0] == "version":
		if f.rcloneOK {
			return "rclone " + rcloneVersion + "\n- os/version: ubuntu 24.04 (64 bit)\n", nil
		}
		if f.rcloneOld {
			return "rclone v1.60.1-DEV\n- os/version: ubuntu 24.04 (64 bit)\n", nil
		}
		return "", errors.New("rclone not installed")
	}

	if f.failContains != "" && script != "" && strings.Contains(script, f.failContains) {
		if f.failTimes == 0 || f.failSeen < f.failTimes {
			f.failSeen++
			out := "partial stdout before failure"
			if f.failOutput != "" {
				out = f.failOutput
			}
			return out, errors.New("exit status 1 (stderr: boom)")
		}
	}
	if strings.HasPrefix(script, "cp /tmp/rclone-"+rcloneVersion+"-linux-amd64/rclone ") {
		f.rcloneOK = true
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
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("ready host → ready, got %s", obs.ObservedState)
	}
	for _, c := range calls {
		if strings.Contains(c, "apt-get -o DPkg::Lock::Timeout") || strings.Contains(c, "get.docker.com") ||
			strings.Contains(c, "nvidia-ctk runtime configure") || strings.Contains(c, rcloneArchive) {
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
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("fresh host → ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	dockerAt := strings.Index(j, "get.docker.com")
	nvidiaAt := strings.Index(j, "nvidia-ctk runtime configure")
	rcloneAt := strings.Index(j, rcloneArchive)
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
		present:       map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
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
		present:       map[string]bool{"gpg": true, "curl": true, "lspci": true},
		gpu:           true,
		dockerUp:      true,
		rcloneOK:      true,
		fuseOK:        true,
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
		failContains: "install -y gnupg",
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
	newManager(fe).Reconcile(context.Background())

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
	obs := newManager(fe).Reconcile(context.Background())
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
	obs := newManager(fe).Reconcile(context.Background())
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
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("stock image → ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	if strings.Contains(j, "apt-get") {
		t.Errorf("stock image must not touch apt at all, calls: %s", j)
	}
	if !strings.Contains(j, rcloneArchive) || !strings.Contains(j, "zipfile") {
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
	obs := newManager(fe).Reconcile(context.Background())
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
	obs := newManager(fe).Reconcile(context.Background())
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
	newManager(fe).Reconcile(context.Background())

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
	newManager(fe).Reconcile(context.Background())

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

func TestReconcileReplacesRcloneOfAnotherVersion(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "fusermount3": true},
		dockerUp:       true,
		rcloneOld:      true,
		fuseOK:         true,
		aptConfPresent: true,
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupReady {
		t.Fatalf("want ready, got %s (err %v)", obs.ObservedState, obs.LastError)
	}
	j := joined(calls)
	if !strings.Contains(j, "https://downloads.rclone.org/"+rcloneVersion+"/"+rcloneArchive) {
		t.Fatalf("rclone of another version must be replaced by the pinned release, calls: %s", j)
	}
	if !strings.Contains(j, rcloneSHA256+"  /tmp/rclone.zip") {
		t.Fatalf("pinned release must be verified by sha256, calls: %s", j)
	}
}

func TestReconcileRefusesRcloneWithWrongChecksum(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "fusermount3": true},
		dockerUp:       true,
		fuseOK:         true,
		aptConfPresent: true,
		failContains:   "sha256sum",
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupError {
		t.Fatalf("checksum mismatch must fail the setup, got %s", obs.ObservedState)
	}
	if strings.Contains(joined(calls), "/usr/bin/") && strings.Contains(joined(calls), "cp ") {
		t.Fatalf("unverified rclone must not be installed, calls: %s", joined(calls))
	}
}

func TestSetupLogKeepsTailWithinBackendLimit(t *testing.T) {
	fe := &fakeExec{
		present:      map[string]bool{"lspci": true},
		dockerUp:     true,
		failContains: "install -y gnupg",
		failOutput:   strings.Repeat("я", 30000) + "last line of apt",
	}
	obs := newManager(fe).Reconcile(context.Background())
	if obs.LastLog == nil {
		t.Fatal("want last_log")
	}
	if n := len(utf16.Encode([]rune(*obs.LastLog))); n > 20000 {
		t.Fatalf("last_log is %d UTF-16 units, backend accepts 20000", n)
	}
	if !strings.Contains(*obs.LastLog, "exit status 1") {
		t.Fatalf("tail with the error must be kept, got ...%s", (*obs.LastLog)[len(*obs.LastLog)-200:])
	}
}

func TestReadyHostIsNeverReinstalled(t *testing.T) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "nvidia-ctk": true, "fusermount3": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
	}
	m := newManager(fe)
	if obs := m.Reconcile(context.Background()); obs.ObservedState != client.SetupReady {
		t.Fatalf("want ready, got %s", obs.ObservedState)
	}
	var emits []string
	m.SetReporter(func(_ context.Context, obs client.AgentSetupObserved) { emits = append(emits, obs.ObservedState) })
	fe.dockerUp = false
	fe.nvidiaRuntime = []bool{false}
	fe.rcloneOK = false
	fe.fuseOK = false
	fe.aptConfPresent = false
	calls = nil

	obs := m.Reconcile(context.Background())

	if obs.ObservedState != client.SetupReady {
		t.Fatalf("host that was ready must stay ready, got %s", obs.ObservedState)
	}
	for _, c := range calls {
		if strings.Contains(c, "get.docker.com") || strings.Contains(c, "systemctl") || strings.Contains(c, "nvidia-ctk runtime configure") ||
			strings.Contains(c, "apt-get") || strings.Contains(c, rcloneArchive) || strings.Contains(c, "printf") || strings.Contains(c, ">> /etc/fuse.conf") {
			t.Fatalf("installer ran on a ready host: %s", c)
		}
	}
	if !strings.Contains(joined(calls), "docker info") {
		t.Fatalf("ready host must still be checked, calls: %s", joined(calls))
	}
	if len(emits) != 0 {
		t.Fatalf("checks on a ready host must not report setup phases, got %v", emits)
	}
}

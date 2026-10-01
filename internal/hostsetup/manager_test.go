package hostsetup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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

	dir              string
	ctdOld           bool
	systemCtd        string
	ownExtracted     bool
	ctdRunsOwn       bool
	ctdStartFails    int
	dumpFails        bool
	containers       string
	storeOld         bool
	storeStuck       bool
	storeSwitched    bool
	dockerDiesOnUndo bool
}

func (f *fakeExec) path(parts ...string) string {
	return filepath.Join(append([]string{f.dir}, parts...)...)
}

func (f *fakeExec) serverVersion(context.Context) (string, error) {
	if f.ctdRunsOwn {
		return containerdVersion, nil
	}
	if f.systemCtd != "" {
		return f.systemCtd, nil
	}
	if f.ctdOld {
		return "v2.2.1", nil
	}
	return containerdVersion, nil
}

func (f *fakeExec) fileHas(path, want string) bool {
	data, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(data), want)
}

func (f *fakeExec) usesContainerdStore() bool {
	return !f.storeOld || f.storeSwitched
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

	case name == containerdBin && len(args) == 1 && args[0] == "--version":
		if f.ownExtracted {
			return "containerd github.com/containerd/containerd/v2 " + containerdVersion + " f2551031d7276a770f65f98c9b52e57e7dad07e8", nil
		}
		return "", errors.New("no such file or directory")

	case (name == containerdBin || name == "containerd") && strings.HasSuffix(full, " config dump"):
		if f.dumpFails {
			return "", errors.New("failed to load TOML: unsupported plugin option")
		}
		out := "version = 4\nimports = ['/etc/containerd/conf.d/*.toml']\n  max_concurrent_unpacks = 1\n"
		if len(args) >= 2 && f.fileHas(args[1], f.path("containerd", "conf.d", "*.toml")) {
			dropIn, _ := os.ReadFile(f.path("containerd", "conf.d", "yougpu.toml"))
			out += string(dropIn)
		}
		return out, nil

	case name == "docker" && len(args) == 2 && args[0] == "ps" && args[1] == "-aq":
		return f.containers, nil

	case name == "systemctl" && len(args) == 2 && args[0] == "start" && args[1] == "containerd":
		if f.ctdStartFails > 0 {
			f.ctdStartFails--
			return "", errors.New("containerd.service: start failed")
		}
		f.ctdRunsOwn = f.ownExtracted && f.fileHas(f.path("systemd", "containerd.service.d", "yougpu.conf"), "ExecStart="+containerdBin)
		return "", nil

	case name == "systemctl" && len(args) == 2 && args[0] == "stop" && args[1] == "docker":
		f.dockerUp = false
		return "", nil

	case name == "docker" && len(args) >= 3 && args[0] == "info" && strings.Contains(args[2], "DriverStatus"):
		if !f.dockerUp {
			return "", errors.New("cannot connect to docker daemon")
		}
		if f.usesContainerdStore() {
			return `[["driver-type","io.containerd.snapshotter.v1"]]`, nil
		}
		return `[["Backing Filesystem","extfs"],["Supports d_type","true"]]`, nil

	case name == "systemctl" && len(args) == 2 && args[0] == "start" && args[1] == "docker":
		wantsStore := f.fileHas(f.path("docker", "daemon.json"), `"containerd-snapshotter": true`)
		if f.dockerDiesOnUndo && !wantsStore {
			return "", errors.New("docker.service: start failed")
		}
		f.dockerUp = true
		f.storeSwitched = wantsStore && !f.storeStuck
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
	if strings.Contains(script, "get.docker.com") {
		f.dockerUp = true
		f.ctdOld = true
		f.storeOld = true
	}
	if strings.Contains(script, "tar -C "+containerdHome) {
		f.ownExtracted = true
	}
	if name == "rm" && strings.Contains(full, containerdHome) {
		f.ownExtracted = false
	}
	return "", nil
}

func newManager(fe *fakeExec) *Manager {
	if fe.dir == "" {
		dir, err := os.MkdirTemp("", "hostsetup-test-")
		if err != nil {
			panic(err)
		}
		fe.dir = dir
	}
	m := NewManager(fe, system.NewSystemd(fe, testLogger()), testLogger())
	m.SetWaitsForTest(0, 5, 3)
	m.SetContainerdPathsForTest(fe.path("containerd", "config.toml"), fe.path("containerd", "conf.d", "yougpu.toml"),
		fe.path("systemd", "containerd.service.d", "yougpu.conf"), fe.path("docker", "daemon.json"))
	m.SetContainerdProbeForTest(fe.serverVersion)
	if fe.dockerUp && !fe.ctdOld && !fe.storeOld {
		writeFile(fe.path("containerd", "config.toml"), "imports = [\""+fe.path("containerd", "conf.d", "*.toml")+"\"]\n")
		writeFile(fe.path("containerd", "conf.d", "yougpu.toml"), containerdDropIn())
		writeFile(fe.path("docker", "daemon.json"), `{"features": {"containerd-snapshotter": true}}`)
	}
	return m
}

func writeFile(path, content string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		panic(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
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

const (
	providerDaemonJSON = `{"registry-mirrors": ["https://mcache-dsm.massedcompute.com"], "runtimes": {"nvidia": {"args": [], "path": "nvidia-container-runtime"}}}`
	providerContainerd = "#   Copyright 2018-2022 Docker Inc.\n\ndisabled_plugins = [\"cri\"]\n\n#root = \"/var/lib/containerd\"\n"
)

func upgradeHost(t *testing.T) (*fakeExec, *[]string) {
	var calls []string
	fe := &fakeExec{
		calls:          &calls,
		dir:            t.TempDir(),
		present:        map[string]bool{"gpg": true, "curl": true, "lspci": true, "nvidia-ctk": true},
		gpu:            true,
		dockerUp:       true,
		rcloneOK:       true,
		fuseOK:         true,
		aptConfPresent: true,
		nvidiaRuntime:  []bool{true},
		ctdOld:         true,
		storeOld:       true,
	}
	writeFile(fe.path("docker", "daemon.json"), providerDaemonJSON)
	writeFile(fe.path("containerd", "config.toml"), providerContainerd)
	return fe, &calls
}

func mustBeReady(t *testing.T, fe *fakeExec) *Manager {
	t.Helper()
	m := newManager(fe)
	if obs := m.Reconcile(context.Background()); obs.ObservedState != client.SetupReady {
		t.Fatalf("expected ready, got %s (err %v)", obs.ObservedState, deref(obs.LastError))
	}
	return m
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func assertDockerAsItWas(t *testing.T, fe *fakeExec) {
	t.Helper()
	if got := readFile(t, fe.path("docker", "daemon.json")); got != providerDaemonJSON {
		t.Errorf("daemon.json must be the provider's again, got %s", got)
	}
	if got := readFile(t, fe.path("containerd", "config.toml")); got != providerContainerd {
		t.Errorf("containerd config must be the provider's again, got %q", got)
	}
	for _, p := range []string{fe.path("containerd", "conf.d", "yougpu.toml"), fe.path("systemd", "containerd.service.d", "yougpu.conf")} {
		if exists(p) {
			t.Errorf("%s must be removed", p)
		}
	}
	if fe.ctdRunsOwn || fe.ownExtracted {
		t.Errorf("own containerd must be gone (running own %v, extracted %v)", fe.ctdRunsOwn, fe.ownExtracted)
	}
	if !fe.dockerUp {
		t.Error("docker must be running")
	}
}

func TestReconcileRunsOwnContainerdBesideTheSystemOne(t *testing.T) {
	fe, calls := upgradeHost(t)
	mustBeReady(t, fe)

	j := joined(*calls)
	for _, want := range []string{"curl -fsSL --retry 3", containerdArchiveURL, containerdSHA256 + "  " + containerdArchive, "tar -C " + containerdHome,
		containerdBin + " --config " + fe.path("containerd", "config.toml") + " config dump"} {
		if !strings.Contains(j, want) {
			t.Errorf("missing %q in calls: %s", want, j)
		}
	}
	if strings.Contains(j, "install -m 755") || strings.Contains(j, "/usr/bin/containerd") {
		t.Errorf("the system containerd binary must not be replaced: %s", j)
	}
	unit := readFile(t, fe.path("systemd", "containerd.service.d", "yougpu.conf"))
	for _, want := range []string{"ExecStart=\nExecStart=" + containerdBin + "\n", "Environment=PATH=" + containerdHome + "/bin:"} {
		if !strings.Contains(unit, want) {
			t.Errorf("systemd drop-in lacks %q:\n%s", want, unit)
		}
	}
	reload := strings.Index(j, "systemctl daemon-reload")
	stopDocker := strings.Index(j, "systemctl stop docker")
	stopCtd := strings.Index(j, "systemctl stop containerd")
	startCtd := strings.Index(j, "systemctl start containerd")
	startDocker := strings.LastIndex(j, "systemctl start docker")
	if !(reload >= 0 && reload < stopDocker && stopDocker < stopCtd && stopCtd < startCtd && startCtd < startDocker) {
		t.Errorf("order must be daemon-reload → stop docker → stop containerd → start containerd → start docker: %s", j)
	}
	if !fe.ctdRunsOwn {
		t.Error("own containerd must be running")
	}
	if got := readFile(t, fe.path("containerd", "conf.d", "yougpu.toml")); got != containerdDropIn() {
		t.Errorf("containerd drop-in = %q", got)
	}
	wantMain := "imports = [\"" + fe.path("containerd", "conf.d", "*.toml") + "\"]\n" + providerContainerd
	if got := readFile(t, fe.path("containerd", "config.toml")); got != wantMain {
		t.Errorf("provider config must only gain the imports line, got %q", got)
	}
	daemon := readFile(t, fe.path("docker", "daemon.json"))
	for _, want := range []string{`"containerd-snapshotter": true`, "mcache-dsm.massedcompute.com", "nvidia-container-runtime"} {
		if !strings.Contains(daemon, want) {
			t.Errorf("daemon.json lost %q: %s", want, daemon)
		}
	}
}

func TestReconcileFreshHostGetsPinnedContainerd(t *testing.T) {
	var calls []string
	fe := &fakeExec{calls: &calls, dir: t.TempDir(), present: map[string]bool{}, gpu: true, nvidiaRuntime: []bool{true}}
	mustBeReady(t, fe)
	j := joined(calls)
	if !(strings.Index(j, "get.docker.com") < strings.Index(j, containerdArchiveURL)) {
		t.Errorf("containerd is added after docker is installed: %s", j)
	}
	if !fe.ctdRunsOwn || !fe.storeSwitched {
		t.Errorf("fresh host must end on own containerd and its store (own %v, store %v)", fe.ctdRunsOwn, fe.storeSwitched)
	}
}

func TestReconcileKeepsSystemContainerdThatIsAlreadyFixed(t *testing.T) {
	fe, calls := upgradeHost(t)
	fe.systemCtd = "v2.5.0"
	mustBeReady(t, fe)
	j := joined(*calls)
	if strings.Contains(j, containerdArchiveURL) || exists(fe.path("systemd", "containerd.service.d", "yougpu.conf")) {
		t.Errorf("a fixed system containerd needs no own copy: %s", j)
	}
	if !strings.Contains(j, "containerd --config "+fe.path("containerd", "config.toml")+" config dump") {
		t.Errorf("the drop-in must be checked with the system containerd: %s", j)
	}
	if !exists(fe.path("containerd", "conf.d", "yougpu.toml")) || !fe.storeSwitched {
		t.Errorf("drop-in and docker store must still be set up")
	}
}

func TestReconcileStaysOnDockerWhenArchiveIsRejected(t *testing.T) {
	fe, calls := upgradeHost(t)
	fe.failContains = "sha256sum -c"
	m := mustBeReady(t, fe)
	j := joined(*calls)
	if strings.Contains(j, "tar -C "+containerdHome) {
		t.Errorf("unverified archive must not be unpacked: %s", j)
	}
	if strings.Contains(j, "systemctl stop docker") {
		t.Errorf("docker must keep running when the archive is rejected: %s", j)
	}
	assertDockerAsItWas(t, fe)

	*calls = nil
	m.Reconcile(context.Background())
	if strings.Contains(joined(*calls), containerdArchiveURL) {
		t.Errorf("a host that gave up must not retry within this run: %s", joined(*calls))
	}
}

func TestReconcileKeepsBrokenDaemonJSONUntouched(t *testing.T) {
	fe, calls := upgradeHost(t)
	path := fe.path("docker", "daemon.json")
	writeFile(path, `{"runtimes": {`)
	mustBeReady(t, fe)
	if got := readFile(t, path); got != `{"runtimes": {` {
		t.Errorf("broken daemon.json must not be rewritten, got %q", got)
	}
	if j := joined(*calls); strings.Contains(j, "systemctl stop docker") || strings.Contains(j, containerdArchiveURL) {
		t.Errorf("docker must be left alone: %s", j)
	}
}

func TestReconcileStaysOnDockerWhenContainerdRejectsItsConfig(t *testing.T) {
	fe, calls := upgradeHost(t)
	fe.dumpFails = true
	mustBeReady(t, fe)
	if strings.Contains(joined(*calls), "systemctl stop docker") {
		t.Errorf("nothing may be restarted with a config containerd cannot load: %s", joined(*calls))
	}
	assertDockerAsItWas(t, fe)
}

func TestReconcileRollsBackWhenDockerStaysOnItsOwnStore(t *testing.T) {
	fe, calls := upgradeHost(t)
	fe.storeStuck = true
	mustBeReady(t, fe)
	assertDockerAsItWas(t, fe)
	if j := joined(*calls); strings.Count(j, "systemctl start containerd") < 2 || strings.Count(j, "systemctl daemon-reload") < 2 {
		t.Errorf("rollback must reload units and restart containerd: %s", j)
	}
}

func TestReconcileRollsBackWhenOwnContainerdDoesNotStart(t *testing.T) {
	fe, _ := upgradeHost(t)
	fe.ctdStartFails = 1
	mustBeReady(t, fe)
	assertDockerAsItWas(t, fe)
}

func TestReconcileFailsWhenDockerDoesNotComeBackAfterRollback(t *testing.T) {
	fe, _ := upgradeHost(t)
	fe.storeStuck = true
	fe.dockerDiesOnUndo = true
	obs := newManager(fe).Reconcile(context.Background())
	if obs.ObservedState != client.SetupError || obs.Detail == nil || *obs.Detail != client.SetupInstallingDocker {
		t.Fatalf("docker that is down after rollback must be an installing_docker error, got %s %v", obs.ObservedState, obs.Detail)
	}
}

func TestReconcileLeavesDockerWithContainersAlone(t *testing.T) {
	fe, calls := upgradeHost(t)
	fe.containers = "3f2a9c1d7e5b\n"
	mustBeReady(t, fe)
	j := joined(*calls)
	if strings.Contains(j, containerdArchiveURL) || strings.Contains(j, "systemctl stop") {
		t.Errorf("switching the store would hide existing containers: %s", j)
	}
	if got := readFile(t, fe.path("docker", "daemon.json")); got != providerDaemonJSON {
		t.Errorf("daemon.json must stay the provider's, got %s", got)
	}
}

func TestReconcileLeavesContainerdWithItsOwnImportsAlone(t *testing.T) {
	fe, calls := upgradeHost(t)
	own := "version = 2\nimports = [\"/etc/containerd/provider.d/*.toml\"]\n"
	writeFile(fe.path("containerd", "config.toml"), own)
	mustBeReady(t, fe)
	if got := readFile(t, fe.path("containerd", "config.toml")); got != own {
		t.Errorf("provider imports must stay untouched, got %q", got)
	}
	if strings.Contains(joined(*calls), "systemctl stop docker") {
		t.Errorf("nothing may be restarted: %s", joined(*calls))
	}
}

func TestReconcileCreatesAndRemovesMissingContainerdConfig(t *testing.T) {
	fe, _ := upgradeHost(t)
	if err := os.Remove(fe.path("containerd", "config.toml")); err != nil {
		t.Fatal(err)
	}
	fe.storeStuck = true
	mustBeReady(t, fe)
	if exists(fe.path("containerd", "config.toml")) {
		t.Error("a containerd config that did not exist must be removed on rollback")
	}

	fe2, _ := upgradeHost(t)
	if err := os.Remove(fe2.path("containerd", "config.toml")); err != nil {
		t.Fatal(err)
	}
	mustBeReady(t, fe2)
	if got := readFile(t, fe2.path("containerd", "config.toml")); !strings.HasPrefix(got, "imports = [") || !fe2.ctdRunsOwn {
		t.Errorf("missing config must be created with the import, got %q (own %v)", got, fe2.ctdRunsOwn)
	}
}

func TestContainerdDropInSetsTransferLimits(t *testing.T) {
	cfg := containerdDropIn()
	for _, want := range []string{"version = 3", "[plugins.'io.containerd.transfer.v1.local']", "max_concurrent_downloads = 6", unpacksLine()} {
		if !strings.Contains(cfg, want) {
			t.Errorf("drop-in lacks %q:\n%s", want, cfg)
		}
	}
}

func TestDockerConfigKeepsEverythingElse(t *testing.T) {
	out, err := withContainerdStore([]byte(`{"runtimes": {"nvidia": {"path": "nvidia-container-runtime"}}, "features": {"buildkit": true}}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"containerd-snapshotter": true`, `"buildkit": true`, "nvidia-container-runtime"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("daemon.json lacks %q: %s", want, out)
		}
	}
	if out, err := withContainerdStore(nil); err != nil || !strings.Contains(string(out), `"containerd-snapshotter": true`) {
		t.Errorf("missing daemon.json must be created, got %s %v", out, err)
	}
	if _, err := withContainerdStore([]byte("{")); err == nil {
		t.Error("broken daemon.json must be an error")
	}
}

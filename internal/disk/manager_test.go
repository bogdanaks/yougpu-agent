package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

type fakeSystemd struct {
	mu          sync.Mutex
	active      map[string]bool
	invocations map[string]string
	calls       []string
	startErr    error
	activeErr   error
	startCheck  func() error
}

func newFakeSystemd() *fakeSystemd {
	return &fakeSystemd{active: map[string]bool{}, invocations: map[string]string{}}
}

func (f *fakeSystemd) InvocationID(_ context.Context, u string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.invocations[u], nil
}

func (f *fakeSystemd) called(c string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, got := range f.calls {
		if got == c {
			return true
		}
	}
	return false
}

func (f *fakeSystemd) record(c string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *fakeSystemd) DaemonReload(context.Context) error { f.record("daemon-reload"); return nil }
func (f *fakeSystemd) Enable(_ context.Context, u string) error {
	f.record("enable:" + u)
	return nil
}
func (f *fakeSystemd) Disable(_ context.Context, u string) error {
	f.record("disable:" + u)
	return nil
}
func (f *fakeSystemd) Start(_ context.Context, u string) error {
	f.record("start:" + u)
	if f.startErr != nil {
		return f.startErr
	}
	if f.startCheck != nil {
		if err := f.startCheck(); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.active[u] = true
	f.mu.Unlock()
	return nil
}
func (f *fakeSystemd) Stop(_ context.Context, u string) error {
	f.record("stop:" + u)
	f.mu.Lock()
	delete(f.active, u)
	f.mu.Unlock()
	return nil
}
func (f *fakeSystemd) IsActive(_ context.Context, u string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.activeErr != nil {
		return false, f.activeErr
	}
	return f.active[u], nil
}
func (f *fakeSystemd) Poweroff(context.Context) error { f.record("poweroff"); return nil }

type fakeExec struct {
	mu    sync.Mutex
	calls [][]string
}

func (f *fakeExec) Run(_ context.Context, _ time.Duration, name string, args ...string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string{name}, args...))
	f.mu.Unlock()
	return "Filesystem     1G-blocks  Used Available Use% Mounted on\n/dev/sda1            100G   10G       100G   10% /\n", nil
}

func (f *fakeExec) find(name string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c[0] == name {
			return c
		}
	}
	return nil
}

func newTestManager(t *testing.T) (*Manager, *fakeSystemd, string) {
	m, sd, tmp, _ := newTestManagerExec(t)
	return m, sd, tmp
}

func newTestManagerExec(t *testing.T) (*Manager, *fakeSystemd, string, *fakeExec) {
	t.Helper()
	tmp := t.TempDir()
	sd := newFakeSystemd()
	exec := &fakeExec{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := NewManager(sd, exec, &fakeKeys{}, log)
	m.SetUnitsDir(tmp)
	m.SetRcloneConfigPath(filepath.Join(t.TempDir(), "rclone.conf"))
	m.SetDirectMarkersDir(filepath.Join(t.TempDir(), "mounts"))
	m.settle = 0
	return m, sd, tmp, exec
}

func diskSpec(t *testing.T, id string) client.AgentDiskSpec {
	return client.AgentDiskSpec{
		ID:           id,
		DesiredState: client.DesiredMounted,
		Bucket:       "test-bucket",
		S3Path:       "u/" + id + "/",
		MountPath:    filepath.Join(t.TempDir(), "mount"),
	}
}

func readEnv(t *testing.T, m *Manager, id string) (map[string]string, os.FileMode) {
	t.Helper()
	path := filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-"+id+".env")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("rc env file: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		k, v, _ := strings.Cut(line, "=")
		env[k] = v
	}
	return env, info.Mode().Perm()
}

func unitBody(t *testing.T, dir, id string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, "storage-mount-"+id+".service"))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	return string(body)
}

func TestMountWritesUnitAndStarts(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	spec := client.AgentDiskSpec{
		ID:           "abc",
		DesiredState: client.DesiredMounted,
		Bucket:       "test-bucket",
		S3Path:       "u/abc/",
		MountPath:    filepath.Join(t.TempDir(), "mount"),
	}

	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatalf("mount: %v", err)
	}

	unitName := "storage-mount-abc.service"
	unitPath := filepath.Join(tmp, unitName)
	body, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	if !bytes.Contains(body, []byte("disk-abc:test-bucket/u/abc/")) {
		t.Errorf("unit missing rclone path:\n%s", body)
	}
	if !bytes.Contains(body, []byte(spec.MountPath)) {
		t.Errorf("unit missing mount path:\n%s", body)
	}

	want := []string{"daemon-reload", "enable:" + unitName, "start:" + unitName}
	for _, c := range want {
		found := false
		for _, got := range sd.calls {
			if got == c {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("systemd call %q not made; calls=%v", c, sd.calls)
		}
	}
}

func TestMountUnitReadsInParallelAndKeepsUploadBuffersSmall(t *testing.T) {
	m, _, tmp := newTestManager(t)
	if err := m.Mount(context.Background(), diskSpec(t, "abc")); err != nil {
		t.Fatalf("mount: %v", err)
	}

	body := unitBody(t, tmp, "abc")
	for _, flag := range []string{"--vfs-read-chunk-streams 32", "--vfs-read-chunk-size 4M"} {
		if !strings.Contains(body, flag) {
			t.Errorf("unit missing %s:\n%s", flag, body)
		}
	}
	if strings.Contains(body, "--vfs-read-chunk-size-limit") {
		t.Errorf("chunk size limit has no effect with parallel chunk streams:\n%s", body)
	}
	transfers := flagValue(t, body, "--transfers")
	concurrency := flagValue(t, body, "--s3-upload-concurrency")
	chunk := flagValue(t, body, "--s3-chunk-size")
	chunkMiB, err := strconv.Atoi(strings.TrimSuffix(chunk, "M"))
	if err != nil || !strings.HasSuffix(chunk, "M") {
		t.Fatalf("chunk size %q", chunk)
	}
	tr, _ := strconv.Atoi(transfers)
	cc, _ := strconv.Atoi(concurrency)
	if buffers := tr * cc * chunkMiB; buffers <= 0 || buffers > 1024 {
		t.Fatalf("upload buffers %d MiB (transfers %s × concurrency %s × chunk %s) must stay within 1 GiB", buffers, transfers, concurrency, chunk)
	}
}

func flagValue(t *testing.T, body, flag string) string {
	t.Helper()
	fields := strings.Fields(body)
	for i, f := range fields {
		if f == flag && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	t.Fatalf("unit has no %s:\n%s", flag, body)
	return ""
}

func TestMountUnitKeepsCacheOffTheLastGigabytesOfTheRootDisk(t *testing.T) {
	m, _, tmp := newTestManager(t)
	if err := m.Mount(context.Background(), diskSpec(t, "abc")); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if v := flagValue(t, unitBody(t, tmp, "abc"), "--vfs-cache-min-free-space"); v != "15G" {
		t.Fatalf("--vfs-cache-min-free-space = %s", v)
	}
}

func TestMountUnitStartsBeforeDockerAndSurvivesOOM(t *testing.T) {
	m, _, tmp := newTestManager(t)
	if err := m.Mount(context.Background(), diskSpec(t, "abc")); err != nil {
		t.Fatalf("mount: %v", err)
	}
	body := unitBody(t, tmp, "abc")
	for _, line := range []string{"Before=docker.service", "OOMScoreAdjust=-900"} {
		if !strings.Contains(body, "\n"+line+"\n") {
			t.Errorf("unit missing %s:\n%s", line, body)
		}
	}
}

func TestMountUnitKeepsRcCredentialsInPrivateEnvFile(t *testing.T) {
	m, _, tmp := newTestManager(t)
	if err := m.Mount(context.Background(), diskSpec(t, "abc")); err != nil {
		t.Fatalf("mount: %v", err)
	}

	body := unitBody(t, tmp, "abc")
	env, mode := readEnv(t, m, "abc")
	if mode != 0o600 {
		t.Fatalf("rc env file mode %o, want 600", mode)
	}
	if len(env["RCLONE_RC_USER"]) < 16 || len(env["RCLONE_RC_PASS"]) < 32 {
		t.Fatalf("rc credentials too short: %v", env)
	}
	if !strings.HasPrefix(env["RCLONE_RC_ADDR"], "127.0.0.1:") {
		t.Fatalf("rc must listen on loopback, got %q", env["RCLONE_RC_ADDR"])
	}
	if strings.Contains(body, "--rc-no-auth") {
		t.Fatalf("rc without auth:\n%s", body)
	}
	if strings.Contains(body, env["RCLONE_RC_PASS"]) || strings.Contains(body, env["RCLONE_RC_USER"]) {
		t.Fatalf("rc credentials leaked into the world-readable unit:\n%s", body)
	}
	envFile := filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-abc.env")
	if !strings.Contains(body, "\nEnvironmentFile="+envFile+"\n") || !strings.Contains(body, " --rc") {
		t.Fatalf("unit must start rc from the env file:\n%s", body)
	}
}

func TestRcCredentialsAndPortsDifferPerDisk(t *testing.T) {
	m, _, _ := newTestManager(t)
	for _, id := range []string{"a", "b", "c"} {
		if err := m.Mount(context.Background(), diskSpec(t, id)); err != nil {
			t.Fatalf("mount %s: %v", id, err)
		}
	}
	ports := map[string]bool{}
	passes := map[string]bool{}
	for _, id := range []string{"a", "b", "c"} {
		env, _ := readEnv(t, m, id)
		ports[env["RCLONE_RC_ADDR"]] = true
		passes[env["RCLONE_RC_PASS"]] = true
	}
	if len(ports) != 3 || len(passes) != 3 {
		t.Fatalf("each disk needs its own rc port and password: ports=%v", ports)
	}
}

func TestRcPortSkipsPortTakenByAnotherProcess(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	busy := l.Addr().(*net.TCPAddr).Port
	m, _, _ := newTestManager(t)
	m.SetRcPortBase(busy)

	if err := m.Mount(context.Background(), diskSpec(t, "abc")); err != nil {
		t.Fatalf("mount: %v", err)
	}
	env, _ := readEnv(t, m, "abc")
	if env["RCLONE_RC_ADDR"] == "127.0.0.1:"+strconv.Itoa(busy) {
		t.Fatalf("rc got a port another process listens on: %s", env["RCLONE_RC_ADDR"])
	}
}

func TestRemountKeepsRcCredentials(t *testing.T) {
	m, sd, _ := newTestManager(t)
	spec := diskSpec(t, "abc")
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	first, _ := readEnv(t, m, "abc")
	sd.mu.Lock()
	delete(sd.active, "storage-mount-abc.service")
	sd.mu.Unlock()
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	second, _ := readEnv(t, m, "abc")
	if first["RCLONE_RC_PASS"] != second["RCLONE_RC_PASS"] || first["RCLONE_RC_ADDR"] != second["RCLONE_RC_ADDR"] {
		t.Fatalf("remount changed rc access: %v → %v", first, second)
	}
}

func TestDirectModeMountsWithTheUnitFlags(t *testing.T) {
	m, _, tmp, exec := newTestManagerExec(t)
	spec := diskSpec(t, "abc")
	if err := m.Mount(context.Background(), spec); err != nil {
		t.Fatalf("mount: %v", err)
	}
	unitArgs := strings.Fields(strings.TrimPrefix(execStart(t, unitBody(t, tmp, "abc")), "/usr/bin/rclone "))

	direct, _, _, directExec := newTestManagerExec(t)
	direct.SetRcloneConfigPath(m.rcloneConfigPath)
	direct.SetDirectMode(true)
	if err := direct.Mount(context.Background(), spec); err != nil {
		t.Fatalf("direct mount: %v", err)
	}
	call := directExec.find("/usr/bin/rclone")
	if call == nil {
		t.Fatalf("direct mode must run the pinned rclone, calls: %v", directExec.calls)
	}
	want := append(slices.DeleteFunc(slices.Clone(unitArgs), func(a string) bool { return a == "--rc" }), "--daemon")
	if !slices.Equal(call[1:], want) {
		t.Fatalf("direct mode flags differ from the unit:\n direct %v\n unit   %v", call[1:], want)
	}
	_ = exec
}

func execStart(t *testing.T, body string) string {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "ExecStart=") {
			return strings.TrimPrefix(line, "ExecStart=")
		}
	}
	t.Fatalf("no ExecStart:\n%s", body)
	return ""
}

func TestMountIDFollowsTheUnitInvocation(t *testing.T) {
	m, sd, _ := newTestManager(t)
	sd.invocations["storage-mount-abc.service"] = "inv-1"

	id, err := m.MountID(context.Background(), "abc")
	if err != nil || id != "inv-1" {
		t.Fatalf("mount id = %q, %v", id, err)
	}

	m.SetDirectMode(true)
	if id, err := m.MountID(context.Background(), "abc"); err != nil || id != "" {
		t.Fatalf("direct mode has no unit invocations, got %q, %v", id, err)
	}
}

func TestEnsureRunningStartsStoppedUnitOnly(t *testing.T) {
	m, sd, _ := newTestManager(t)
	sd.active["storage-mount-up.service"] = true

	if err := m.EnsureRunning(context.Background(), "up"); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureRunning(context.Background(), "down"); err != nil {
		t.Fatal(err)
	}

	if sd.called("start:storage-mount-up.service") {
		t.Fatal("running unit restarted")
	}
	if !sd.called("start:storage-mount-down.service") {
		t.Fatalf("stopped unit must be started to finish its uploads, calls=%v", sd.calls)
	}
}

func TestUnmountRemovesUnit(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	unitName := "storage-mount-xyz.service"
	if err := os.WriteFile(filepath.Join(tmp, unitName), []byte("placeholder"), 0o644); err != nil {
		t.Fatal(err)
	}
	sd.active[unitName] = true
	withRc(t, m, "xyz", rcStats(`{"diskCache":{"uploadsInProgress":0,"uploadsQueued":0}}`, `{}`))

	if err := m.Unmount(context.Background(), "xyz"); err != nil {
		t.Fatalf("unmount: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, unitName)); !os.IsNotExist(err) {
		t.Errorf("unit file still present: %v", err)
	}
	joined := strings.Join(sd.calls, ",")
	if !strings.Contains(joined, "stop:"+unitName) {
		t.Errorf("expected stop call, calls=%v", sd.calls)
	}
}

func TestListUnits(t *testing.T) {
	m, _, tmp := newTestManager(t)
	files := []string{
		"storage-mount-a.service",
		"storage-mount-bbb.service",
		"unrelated.service",
		"storage-mount-x.timer",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tmp, f), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := m.ListUnits()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 ids, got %v", ids)
	}
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	if !set["a"] || !set["bbb"] {
		t.Errorf("unexpected ids: %v", ids)
	}
}

func TestUnmountKeepsRunningDiskWhenStateUnknown(t *testing.T) {
	m, sd, tmp := newTestManager(t)
	activeUnit(t, m, sd, tmp, "d1")
	writeRcEnv(t, m, "d1", "127.0.0.1:1", "u", "p")
	sd.activeErr = errors.New("systemctl is-active storage-mount-d1.service: signal: killed")

	if err := m.Unmount(context.Background(), "d1"); err == nil {
		t.Fatal("unmount must fail while the unit state is unknown")
	}

	for _, c := range []string{"stop:storage-mount-d1.service", "disable:storage-mount-d1.service"} {
		if sd.called(c) {
			t.Fatalf("%s called for a unit that may be running", c)
		}
	}
	if _, err := os.Stat(filepath.Join(tmp, "storage-mount-d1.service")); err != nil {
		t.Fatalf("unit file of a running rclone removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(m.rcloneConfigPath), "rc-d1.env")); err != nil {
		t.Fatalf("rc env of a running rclone removed: %v", err)
	}
}

package hostsetup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/ctrd"
	"github.com/bogdanaks/yougpu-agent/internal/fetch"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	aptTimeout      = 10 * time.Minute
	aptLockTimeoutS = 300
	aptConfPath     = "/etc/apt/apt.conf.d/99yougpu-provisioning"
	dockerInstallTO = 10 * time.Minute
	dockerInfoTO    = 15 * time.Second
	cmdTimeout      = 30 * time.Second
	downloadTimeout = 5 * time.Minute
	agentLogPath    = "/var/log/yougpu-agent.log"
	unzipViaPython  = `python3 -c "import zipfile; zipfile.ZipFile('/tmp/rclone.zip').extractall('/tmp')"`
	maxLastError    = 1024
	maxLogTail      = 4096
	maxLastLog      = 20000

	rcloneVersion = "v1.75.1"
	rcloneSHA256  = "982b5aa772841168f8e380f139e9e787b2a105403e32b94da8676a0e1c0a13ab"
	rcloneArchive = "rclone-" + rcloneVersion + "-linux-amd64.zip"
	rcloneBin     = "/usr/bin/rclone"

	containerdVersion    = "v2.4.1"
	containerdSHA256     = "d65eda6a188aac1006848d8060099be88ba9c4bcddbfd9a23a961194710d0dd4"
	containerdArchiveURL = "https://github.com/containerd/containerd/releases/download/" + containerdVersion + "/containerd-2.4.1-linux-amd64.tar.gz"
	containerdArchive    = "/tmp/containerd.tar.gz"
	containerdHome       = "/opt/yougpu/containerd"
	containerdBin        = containerdHome + "/bin/containerd"
	containerdUnpacks    = 6
	containerdDownloads  = 6
	containerdStoreType  = "io.containerd.snapshotter.v1"
	containerdMainConfig = "/etc/containerd/config.toml"
	containerdDropInFile = "/etc/containerd/conf.d/yougpu.toml"
	containerdUnitDropIn = "/etc/systemd/system/containerd.service.d/yougpu.conf"
	dockerConfigFile     = "/etc/docker/daemon.json"

	defaultNvidiaTries   = 30
	defaultAptTries      = 3
	defaultAptRetryDelay = 15 * time.Second
	defaultPollDelay     = 2 * time.Second
)

type step struct {
	phase string
	skip  func(ctx context.Context) bool
	run   func(ctx context.Context) error
}

type Manager struct {
	exec     system.Executor
	systemd  system.Systemd
	log      *slog.Logger
	reporter func(context.Context, client.AgentSetupObserved)

	lastOutput   string
	reachedReady bool

	pollDelay     time.Duration
	nvidiaTries   int
	aptTries      int
	aptRetryDelay time.Duration

	containerdMainPath   string
	containerdDropInPath string
	unitDropInPath       string
	dockerConfigPath     string
	containerdVersion    func(context.Context) (string, error)
	containerdSkipped    bool
}

func NewManager(exec system.Executor, systemd system.Systemd, log *slog.Logger) *Manager {
	return &Manager{
		exec:          exec,
		systemd:       systemd,
		log:           log,
		pollDelay:     defaultPollDelay,
		nvidiaTries:   defaultNvidiaTries,
		aptTries:      defaultAptTries,
		aptRetryDelay: defaultAptRetryDelay,

		containerdMainPath:   containerdMainConfig,
		containerdDropInPath: containerdDropInFile,
		unitDropInPath:       containerdUnitDropIn,
		dockerConfigPath:     dockerConfigFile,
		containerdVersion:    ctrd.ServerVersion,
	}
}

func (m *Manager) SetReporter(fn func(context.Context, client.AgentSetupObserved)) {
	m.reporter = fn
}

func (m *Manager) SetWaitsForTest(pollDelay time.Duration, nvidiaTries, aptTries int) {
	m.pollDelay = pollDelay
	m.nvidiaTries = nvidiaTries
	m.aptTries = aptTries
	m.aptRetryDelay = pollDelay
}

func (m *Manager) SetContainerdPathsForTest(containerdMain, containerdDropIn, unitDropIn, dockerConfig string) {
	m.containerdMainPath = containerdMain
	m.containerdDropInPath = containerdDropIn
	m.unitDropInPath = unitDropIn
	m.dockerConfigPath = dockerConfig
}

func (m *Manager) SetContainerdProbeForTest(fn func(context.Context) (string, error)) {
	m.containerdVersion = fn
}

func (m *Manager) emit(ctx context.Context, obs client.AgentSetupObserved) {
	if m.reporter != nil {
		m.reporter(ctx, obs)
	}
}

func (m *Manager) Reconcile(ctx context.Context) client.AgentSetupObserved {
	if m.reachedReady {
		m.check(ctx)
		return ready()
	}
	m.ensureAptLockConfig(ctx)

	steps := m.steps()
	total := len(steps)
	for i, s := range steps {
		progress := i * 100 / total
		if s.skip(ctx) {
			m.emit(ctx, client.AgentSetupObserved{ObservedState: s.phase, Progress: ptrInt(progress)})
			continue
		}
		m.log.Info("host-setup step starting", "phase", s.phase, "step", i+1, "of", total, "progress", progress)
		m.emit(ctx, client.AgentSetupObserved{ObservedState: s.phase, Progress: ptrInt(progress)})
		if err := s.run(ctx); err != nil {
			m.log.Error("host-setup step failed", "phase", s.phase, "step", i+1, "of", total, "err", err)
			return m.errorObserved(s.phase, err)
		}
	}
	m.reachedReady = true
	m.log.Info("host-setup complete: host ready")
	return ready()
}

func (m *Manager) check(ctx context.Context) {
	for _, s := range m.steps() {
		if s.skip(ctx) {
			continue
		}
		if ctx.Err() == nil {
			m.log.Warn("host-setup check failed on a ready host, installers are not run again", "phase", s.phase)
		}
		return
	}
}

func ready() client.AgentSetupObserved {
	return client.AgentSetupObserved{ObservedState: client.SetupReady, Progress: ptrInt(100)}
}

func (m *Manager) steps() []step {
	return []step{
		{
			phase: client.SetupInstallingBase,
			skip: func(ctx context.Context) bool {
				return m.commandExists(ctx, "gpg") && m.commandExists(ctx, "lspci") && m.commandExists(ctx, "curl")
			},
			run: func(ctx context.Context) error {
				if _, err := m.apt(ctx, "update"); err != nil {
					return err
				}
				_, err := m.apt(ctx, "install -y gnupg pciutils ca-certificates curl")
				return err
			},
		},
		{
			phase: client.SetupInstallingDocker,
			skip: func(ctx context.Context) bool {
				return m.dockerInfoOK(ctx) && (m.containerdSkipped || m.containerdReady(ctx))
			},
			run: m.installDocker,
		},
		{
			phase: client.SetupConfiguringGPU,
			skip: func(ctx context.Context) bool {
				if !m.hasGPU(ctx) {
					return true
				}
				return m.commandExists(ctx, "nvidia-ctk") && m.dockerHasNvidiaRuntime(ctx)
			},
			run: m.configureNvidia,
		},
		{
			phase: client.SetupInstallingStorage,
			skip: func(ctx context.Context) bool {
				return m.rcloneOK(ctx) && m.fuseConfigured(ctx)
			},
			run: m.installStorage,
		},
	}
}

func (m *Manager) installDocker(ctx context.Context) error {
	if !m.dockerInfoOK(ctx) {
		if _, err := m.sh(ctx, dockerInstallTO, "curl -fsSL https://get.docker.com | sh"); err != nil {
			return err
		}
		if err := m.systemd.Enable(ctx, "docker"); err != nil {
			return err
		}
		if err := m.systemd.Start(ctx, "docker"); err != nil {
			return err
		}
	}
	return m.switchToContainerd(ctx)
}

func (m *Manager) containerdReady(ctx context.Context) bool {
	version, err := m.containerdVersion(ctx)
	return err == nil && ctrd.SupportsParallelUnpack(version) && m.containerdConfigured() && m.dockerUsesContainerdStore(ctx)
}

func (m *Manager) containerdConfigured() bool {
	data, err := os.ReadFile(m.containerdDropInPath)
	if err != nil || string(data) != containerdDropIn() {
		return false
	}
	main, err := readOptional(m.containerdMainPath)
	if err != nil {
		return false
	}
	imports, err := containerdImports(main)
	return err == nil && slices.Contains(imports, m.dropInGlob())
}

func (m *Manager) dropInGlob() string {
	return filepath.Join(filepath.Dir(m.containerdDropInPath), "*.toml")
}

func (m *Manager) dockerUsesContainerdStore(ctx context.Context) bool {
	out, err := m.exec.Run(ctx, dockerInfoTO, "docker", "info", "--format", "{{json .DriverStatus}}")
	return err == nil && strings.Contains(out, containerdStoreType)
}

func (m *Manager) dockerHasContainers(ctx context.Context) bool {
	out, err := m.exec.Run(ctx, dockerInfoTO, "docker", "ps", "-aq")
	return err != nil || strings.TrimSpace(out) != ""
}

type undoLog struct {
	steps []func() error
}

func (u *undoLog) replace(path string, data []byte) error {
	before, err := readOptional(path)
	if err != nil {
		return err
	}
	if err := writeConfig(path, data); err != nil {
		return err
	}
	u.steps = append(u.steps, func() error { return restoreConfig(path, before) })
	return nil
}

func (u *undoLog) run() error {
	var errs []error
	for i := len(u.steps) - 1; i >= 0; i-- {
		if err := u.steps[i](); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) switchToContainerd(ctx context.Context) error {
	if m.dockerHasContainers(ctx) {
		m.log.Warn("docker already has containers, its image store and containerd stay as they are")
		m.containerdSkipped = true
		return nil
	}
	var undo undoLog
	own, daemon, err := m.prepareContainerd(ctx, &undo)
	if err != nil {
		return m.giveUpContainerd(ctx, &undo, false, err)
	}
	if err := m.applyContainerd(ctx, &undo, own, daemon); err != nil {
		return m.giveUpContainerd(ctx, &undo, true, err)
	}
	return nil
}

func (m *Manager) prepareContainerd(ctx context.Context, undo *undoLog) (bool, []byte, error) {
	current, err := readOptional(m.dockerConfigPath)
	if err != nil {
		return false, nil, err
	}
	daemon, err := withContainerdStore(current)
	if err != nil {
		return false, nil, err
	}
	main, err := readOptional(m.containerdMainPath)
	if err != nil {
		return false, nil, err
	}
	main, err = withImport(main, m.dropInGlob())
	if err != nil {
		return false, nil, err
	}

	bin := "containerd"
	version, err := m.containerdVersion(ctx)
	own := err != nil || !ctrd.SupportsParallelUnpack(version)
	if own {
		bin = containerdBin
		if !m.ownContainerdUnpacked(ctx) {
			if err := m.downloadContainerd(ctx); err != nil {
				return own, nil, err
			}
		}
	}
	if err := undo.replace(m.containerdDropInPath, []byte(containerdDropIn())); err != nil {
		return own, nil, err
	}
	if err := undo.replace(m.containerdMainPath, main); err != nil {
		return own, nil, err
	}
	out, err := m.exec.Run(ctx, cmdTimeout, bin, "--config", m.containerdMainPath, "config", "dump")
	if err != nil {
		return own, nil, fmt.Errorf("containerd не принимает конфиг провайдера: %w", err)
	}
	if !strings.Contains(out, unpacksLine()) {
		return own, nil, fmt.Errorf("containerd не подхватил %s", m.containerdDropInPath)
	}
	return own, daemon, nil
}

func (m *Manager) applyContainerd(ctx context.Context, undo *undoLog, own bool, daemon []byte) error {
	if own {
		if err := undo.replace(m.unitDropInPath, []byte(containerdUnit())); err != nil {
			return err
		}
		if err := m.systemd.DaemonReload(ctx); err != nil {
			return err
		}
	}
	if err := undo.replace(m.dockerConfigPath, daemon); err != nil {
		return err
	}
	if err := m.restartContainerdAndDocker(ctx); err != nil {
		return err
	}
	for i := 0; i < m.nvidiaTries; i++ {
		if m.containerdReady(ctx) {
			m.log.Info("docker keeps images in containerd", "own_containerd", own, "attempt", i+1)
			return nil
		}
		if err := m.pause(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("docker не перешёл на хранилище образов containerd после %d попыток", m.nvidiaTries)
}

func (m *Manager) giveUpContainerd(ctx context.Context, undo *undoLog, restarted bool, cause error) error {
	m.log.Warn("containerd switch failed, docker stays as it was", "err", cause)
	m.containerdSkipped = true
	if err := undo.run(); err != nil {
		return fmt.Errorf("откат containerd: %w (причина: %v)", err, cause)
	}
	if restarted {
		if err := m.systemd.DaemonReload(ctx); err != nil {
			return fmt.Errorf("откат containerd: %w (причина: %v)", err, cause)
		}
		if err := m.restartContainerdAndDocker(ctx); err != nil {
			return fmt.Errorf("откат containerd: %w (причина: %v)", err, cause)
		}
	}
	_, _ = m.exec.Run(ctx, cmdTimeout, "rm", "-rf", containerdHome, containerdArchive)
	for i := 0; i < m.nvidiaTries; i++ {
		if m.dockerInfoOK(ctx) {
			return nil
		}
		if err := m.pause(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("docker не поднялся после отката containerd (причина: %v)", cause)
}

func (m *Manager) restartContainerdAndDocker(ctx context.Context) error {
	if err := m.systemd.Stop(ctx, "docker"); err != nil {
		return err
	}
	err := m.systemd.Stop(ctx, "containerd")
	if err == nil {
		err = m.systemd.Start(ctx, "containerd")
	}
	if startErr := m.systemd.Start(ctx, "docker"); err == nil {
		err = startErr
	}
	return err
}

func (m *Manager) pause(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.pollDelay):
		return nil
	}
}

func (m *Manager) ownContainerdUnpacked(ctx context.Context) bool {
	out, err := m.exec.Run(ctx, cmdTimeout, containerdBin, "--version")
	return err == nil && strings.Contains(out, " "+containerdVersion+" ")
}

func (m *Manager) downloadContainerd(ctx context.Context) error {
	if _, err := m.sh(ctx, downloadTimeout, "curl -fsSL --retry 3 --retry-delay 2 --retry-connrefused -o "+containerdArchive+" "+containerdArchiveURL); err != nil {
		return err
	}
	if _, err := m.sh(ctx, cmdTimeout, "echo '"+containerdSHA256+"  "+containerdArchive+"' | sha256sum -c -"); err != nil {
		return fmt.Errorf("containerd %s: контрольная сумма архива не совпала: %w", containerdVersion, err)
	}
	_, err := m.sh(ctx, cmdTimeout, "rm -rf "+containerdHome+" && mkdir -p "+containerdHome+
		" && tar -C "+containerdHome+" -xzf "+containerdArchive+" && rm -f "+containerdArchive)
	return err
}

func unpacksLine() string {
	return fmt.Sprintf("max_concurrent_unpacks = %d", containerdUnpacks)
}

func containerdDropIn() string {
	return "version = 3\n\n" +
		"[plugins.'io.containerd.transfer.v1.local']\n" +
		fmt.Sprintf("  max_concurrent_downloads = %d\n", containerdDownloads) +
		"  " + unpacksLine() + "\n"
}

func containerdUnit() string {
	return "[Service]\n" +
		"Environment=PATH=" + containerdHome + "/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n" +
		"ExecStart=\n" +
		"ExecStart=" + containerdBin + "\n"
}

func containerdImports(main []byte) ([]string, error) {
	var cfg struct {
		Imports []string `toml:"imports"`
	}
	if err := toml.Unmarshal(main, &cfg); err != nil {
		return nil, fmt.Errorf("конфиг containerd не разбирается: %w", err)
	}
	return cfg.Imports, nil
}

func withImport(main []byte, glob string) ([]byte, error) {
	imports, err := containerdImports(main)
	if err != nil {
		return nil, err
	}
	if slices.Contains(imports, glob) {
		return main, nil
	}
	if len(imports) > 0 {
		return nil, fmt.Errorf("у containerd уже свои imports %v", imports)
	}
	return append([]byte(fmt.Sprintf("imports = [%q]\n", glob)), main...), nil
}

func withContainerdStore(current []byte) ([]byte, error) {
	cfg := map[string]any{}
	if len(bytes.TrimSpace(current)) > 0 {
		if err := json.Unmarshal(current, &cfg); err != nil {
			return nil, fmt.Errorf("daemon.json не разбирается: %w", err)
		}
	}
	features, _ := cfg["features"].(map[string]any)
	if features == nil {
		features = map[string]any{}
	}
	features["containerd-snapshotter"] = true
	cfg["features"] = features
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

func readOptional(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func restoreConfig(path string, before []byte) error {
	if before == nil {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return writeConfig(path, before)
}

func writeConfig(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".yougpu-tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *Manager) configureNvidia(ctx context.Context) error {
	if !m.commandExists(ctx, "nvidia-ctk") {
		if _, err := m.sh(ctx, aptTimeout, "curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg"); err != nil {
			return err
		}
		if _, err := m.sh(ctx, aptTimeout, "curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | tee /etc/apt/sources.list.d/nvidia-container-toolkit.list"); err != nil {
			return err
		}
		if _, err := m.apt(ctx, "update"); err != nil {
			return err
		}
		if _, err := m.apt(ctx, "install -y nvidia-container-toolkit"); err != nil {
			return err
		}
	}

	if _, err := m.sh(ctx, cmdTimeout, "nvidia-ctk runtime configure --runtime=docker --set-as-default"); err != nil {
		return err
	}
	if err := m.systemd.Stop(ctx, "docker"); err != nil {
		return err
	}
	if err := m.systemd.Start(ctx, "docker"); err != nil {
		return err
	}

	for i := 0; i < m.nvidiaTries; i++ {
		if m.dockerHasNvidiaRuntime(ctx) {
			m.log.Info("nvidia runtime ready", "attempt", i+1)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.pollDelay):
		}
	}
	return fmt.Errorf("nvidia runtime did not appear after %d attempts", m.nvidiaTries)
}

func (m *Manager) installStorage(ctx context.Context) error {
	if !m.fuseAvailable(ctx) {
		if _, err := m.apt(ctx, "update"); err != nil {
			return err
		}
		if _, err := m.apt(ctx, "install -y fuse3"); err != nil {
			if _, err := m.apt(ctx, "install -y fuse"); err != nil {
				return err
			}
		}
	}
	if _, err := m.sh(ctx, cmdTimeout, "grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf"); err != nil {
		return err
	}
	if !m.rcloneOK(ctx) {
		if _, err := m.sh(ctx, downloadTimeout, "curl -fsSL --retry 3 --retry-delay 2 --retry-connrefused -o /tmp/rclone.zip https://downloads.rclone.org/"+rcloneVersion+"/"+rcloneArchive); err != nil {
			return err
		}
		if _, err := m.sh(ctx, cmdTimeout, "echo '"+rcloneSHA256+"  /tmp/rclone.zip' | sha256sum -c -"); err != nil {
			_, _ = m.exec.Run(ctx, cmdTimeout, "rm", "-f", "/tmp/rclone.zip")
			return fmt.Errorf("rclone %s: контрольная сумма архива не совпала: %w", rcloneVersion, err)
		}
		if err := m.unzipRclone(ctx); err != nil {
			return err
		}
		dir := "/tmp/rclone-" + rcloneVersion + "-linux-amd64"
		if _, err := m.sh(ctx, cmdTimeout, "cp "+dir+"/rclone "+rcloneBin+" && chown root:root "+rcloneBin+" && chmod 755 "+rcloneBin+" && rm -rf /tmp/rclone.zip "+dir); err != nil {
			return err
		}
		if !m.rcloneOK(ctx) {
			return fmt.Errorf("rclone %s не установился", rcloneVersion)
		}
	}
	_, err := m.sh(ctx, cmdTimeout, "mkdir -p /root/.config/rclone")
	return err
}

func (m *Manager) unzipRclone(ctx context.Context) error {
	if _, err := m.sh(ctx, cmdTimeout, unzipViaPython); err == nil {
		return nil
	}
	if !m.commandExists(ctx, "unzip") {
		if _, err := m.apt(ctx, "update"); err != nil {
			return err
		}
		if _, err := m.apt(ctx, "install -y unzip"); err != nil {
			return err
		}
	}
	_, err := m.sh(ctx, cmdTimeout, "unzip -q -o /tmp/rclone.zip -d /tmp/")
	return err
}

func (m *Manager) fuseAvailable(ctx context.Context) bool {
	return m.commandExists(ctx, "fusermount3") || m.commandExists(ctx, "fusermount")
}

func (m *Manager) apt(ctx context.Context, args string) (string, error) {
	script := fmt.Sprintf("DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=%d %s", aptLockTimeoutS, args)

	var out string
	var err error
	for i := 0; i < m.aptTries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(m.aptRetryDelay):
			}
		}
		if out, err = m.sh(ctx, aptTimeout, script); err == nil {
			return out, nil
		}
		m.log.Warn("apt-get failed", "args", args, "attempt", i+1, "of", m.aptTries, "err", err)
	}
	return out, err
}

func (m *Manager) ensureAptLockConfig(ctx context.Context) {
	if _, err := m.exec.Run(ctx, cmdTimeout, "sh", "-c", "test -f "+aptConfPath); err == nil {
		return
	}
	script := fmt.Sprintf("printf 'DPkg::Lock::Timeout \"%d\";\\n' > %s", aptLockTimeoutS, aptConfPath)
	if _, err := m.exec.Run(ctx, cmdTimeout, "sh", "-c", script); err != nil {
		m.log.Warn("apt lock config write failed", "path", aptConfPath, "err", err)
	}
}

func (m *Manager) commandExists(ctx context.Context, name string) bool {
	_, err := m.exec.Run(ctx, cmdTimeout, "sh", "-c", "command -v "+name)
	return err == nil
}

func (m *Manager) dockerInfoOK(ctx context.Context) bool {
	_, err := m.exec.Run(ctx, dockerInfoTO, "docker", "info")
	return err == nil
}

func (m *Manager) dockerHasNvidiaRuntime(ctx context.Context) bool {
	out, err := m.exec.Run(ctx, dockerInfoTO, "docker", "info", "--format", "{{.Runtimes}}")
	if err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(out), "nvidia")
}

func (m *Manager) hasGPU(ctx context.Context) bool {
	_, err := m.exec.Run(ctx, cmdTimeout, "sh", "-c", "lspci | grep -i nvidia")
	return err == nil
}

func (m *Manager) rcloneOK(ctx context.Context) bool {
	out, err := m.exec.Run(ctx, cmdTimeout, rcloneBin, "version")
	if err != nil {
		return false
	}
	first, _, _ := strings.Cut(out, "\n")
	return strings.TrimSpace(first) == "rclone "+rcloneVersion
}

func (m *Manager) fuseConfigured(ctx context.Context) bool {
	_, err := m.exec.Run(ctx, cmdTimeout, "sh", "-c", "grep -q '^user_allow_other' /etc/fuse.conf")
	return err == nil
}

func (m *Manager) sh(ctx context.Context, timeout time.Duration, script string) (string, error) {
	out, err := m.exec.Run(ctx, timeout, "sh", "-c", script)
	m.lastOutput = out
	return out, err
}

func (m *Manager) errorObserved(phase string, err error) client.AgentSetupObserved {
	short := fetch.Clip(err.Error(), maxLastError)
	bundle := fetch.ClipTail(m.diagnosticBundle(err), maxLastLog)
	return client.AgentSetupObserved{
		ObservedState: client.SetupError,
		Detail:        ptrStr(phase),
		LastError:     &short,
		LastLog:       &bundle,
	}
}

func (m *Manager) diagnosticBundle(err error) string {
	var b strings.Builder
	if strings.TrimSpace(m.lastOutput) != "" {
		b.WriteString("--- command output ---\n")
		b.WriteString(m.lastOutput)
		b.WriteString("\n")
	}
	b.WriteString("--- error ---\n")
	b.WriteString(err.Error())
	if tail := m.agentLogTail(); tail != "" {
		b.WriteString("\n--- agent log tail ---\n")
		b.WriteString(tail)
	}
	return b.String()
}

func (m *Manager) agentLogTail() string {
	data, err := os.ReadFile(agentLogPath)
	if err != nil {
		return ""
	}
	if len(data) > maxLogTail {
		data = data[len(data)-maxLogTail:]
	}
	return string(data)
}

func ptrInt(v int) *int {
	return &v
}

func ptrStr(v string) *string {
	return &v
}

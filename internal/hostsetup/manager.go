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
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
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

	dockerConfigPath   = "/etc/docker/daemon.json"
	dockerDownloadsKey = "max-concurrent-downloads"
	dockerDownloads    = 8
	dockerRestartTries = 30

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
	dockerConfig string

	pollDelay     time.Duration
	nvidiaTries   int
	aptTries      int
	aptRetryDelay time.Duration
}

func NewManager(exec system.Executor, systemd system.Systemd, log *slog.Logger) *Manager {
	return &Manager{
		exec:          exec,
		systemd:       systemd,
		log:           log,
		dockerConfig:  dockerConfigPath,
		pollDelay:     defaultPollDelay,
		nvidiaTries:   defaultNvidiaTries,
		aptTries:      defaultAptTries,
		aptRetryDelay: defaultAptRetryDelay,
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

func (m *Manager) SetDockerConfigForTest(path string) {
	m.dockerConfig = path
}

func (m *Manager) emit(ctx context.Context, obs client.AgentSetupObserved) {
	if m.reporter != nil {
		m.reporter(ctx, obs)
	}
}

func (m *Manager) Reconcile(ctx context.Context) client.AgentSetupObserved {
	m.ensureAptLockConfig(ctx)

	steps := m.steps()
	total := len(steps)
	for i, s := range steps {
		progress := i * 100 / total
		if s.skip(ctx) {
			if !m.reachedReady {
				m.emit(ctx, client.AgentSetupObserved{ObservedState: s.phase, Progress: ptrInt(progress)})
			}
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
				return m.dockerInfoOK(ctx) && m.dockerTuned()
			},
			run: func(ctx context.Context) error {
				if !m.dockerInfoOK(ctx) {
					if err := m.installDocker(ctx); err != nil {
						return err
					}
				}
				if m.dockerTuned() {
					return nil
				}
				return m.tuneDocker(ctx)
			},
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
	if _, err := m.sh(ctx, dockerInstallTO, "curl -fsSL https://get.docker.com | sh"); err != nil {
		return err
	}
	if err := m.systemd.Enable(ctx, "docker"); err != nil {
		return err
	}
	return m.systemd.Start(ctx, "docker")
}

func (m *Manager) readDockerConfig() (map[string]any, error) {
	cfg := map[string]any{}
	data, err := os.ReadFile(m.dockerConfig)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", m.dockerConfig, err)
	}
	return cfg, nil
}

func (m *Manager) dockerTuned() bool {
	cfg, err := m.readDockerConfig()
	if err != nil {
		return false
	}
	downloads, _ := cfg[dockerDownloadsKey].(float64)
	return downloads >= dockerDownloads
}

func (m *Manager) tuneDocker(ctx context.Context) error {
	cfg, err := m.readDockerConfig()
	if err != nil {
		return err
	}
	cfg[dockerDownloadsKey] = dockerDownloads
	data, err := json.MarshalIndent(cfg, "", "    ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.dockerConfig), 0o755); err != nil {
		return err
	}
	tmp := m.dockerConfig + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.dockerConfig); err != nil {
		return err
	}
	m.log.Info("docker pull concurrency raised", "max_concurrent_downloads", dockerDownloads)

	if err := m.systemd.Stop(ctx, "docker"); err != nil {
		return err
	}
	if err := m.systemd.Start(ctx, "docker"); err != nil {
		return err
	}
	for i := 0; i < dockerRestartTries; i++ {
		if m.dockerInfoOK(ctx) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.pollDelay):
		}
	}
	return fmt.Errorf("docker did not come back after %d attempts", dockerRestartTries)
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
		if _, err := m.sh(ctx, downloadTimeout, "curl -fsSL -o /tmp/rclone.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip"); err != nil {
			return err
		}
		if err := m.unzipRclone(ctx); err != nil {
			return err
		}
		if _, err := m.sh(ctx, cmdTimeout, "cp /tmp/rclone-*-linux-amd64/rclone /usr/bin/ && chown root:root /usr/bin/rclone && chmod 755 /usr/bin/rclone && rm -rf /tmp/rclone.zip /tmp/rclone-*-linux-amd64"); err != nil {
			return err
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
	_, err := m.exec.Run(ctx, cmdTimeout, "rclone", "version")
	return err == nil
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
	short := truncate(err.Error(), maxLastError)
	bundle := m.diagnosticBundle(err)
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

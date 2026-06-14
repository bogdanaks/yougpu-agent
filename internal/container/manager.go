package container

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	containerName   = "app_container"
	labelManaged    = "yougpu.managed"
	labelSpecHash   = "yougpu.spec.hash"
	dockerTimeout   = 5 * time.Second
	pullTimeout     = 10 * time.Minute
	runTimeout      = 2 * time.Minute
	reportThrottle  = 5 * time.Second
	inspectNoExit   = "no such object"
	inspectNotFound = "No such object"
)

type Action int

const (
	ActionNone Action = iota
	ActionApply
	ActionRemove
)

type Observed struct {
	Exists   bool
	Running  bool
	SpecHash string
}

type Manager struct {
	exec     system.Executor
	puller   Puller
	reporter func(context.Context, client.AgentContainerObserved)
	log      *slog.Logger
	name     string
}

func NewManager(exec system.Executor, puller Puller, log *slog.Logger) *Manager {
	return &Manager{exec: exec, puller: puller, log: log, name: containerName}
}

func (m *Manager) SetReporter(fn func(context.Context, client.AgentContainerObserved)) {
	m.reporter = fn
}

func (m *Manager) emit(ctx context.Context, obs client.AgentContainerObserved) {
	if m.reporter != nil {
		m.reporter(ctx, obs)
	}
}

func SpecHash(spec *client.AgentContainerSpec) string {
	if spec == nil {
		return ""
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return strconv.FormatUint(h.Sum64(), 16)
}

func Decide(hasSpec bool, desiredHash string, obs Observed) Action {
	if !hasSpec {
		if obs.Exists {
			return ActionRemove
		}
		return ActionNone
	}
	if obs.Exists && obs.Running && obs.SpecHash == desiredHash {
		return ActionNone
	}
	return ActionApply
}

func (m *Manager) Reconcile(ctx context.Context, spec *client.AgentContainerSpec) client.AgentContainerObserved {
	hasSpec := spec != nil
	desiredHash := SpecHash(spec)

	obs, err := m.observe(ctx)
	if err != nil {
		if hasSpec {
			m.log.Warn("container inspect failed", "err", err)
		} else {
			m.log.Debug("container inspect skipped", "err", err)
		}
	}

	switch Decide(hasSpec, desiredHash, obs) {
	case ActionNone:
		return m.report(hasSpec, desiredHash, obs, nil)
	case ActionRemove:
		m.log.Info("removing unmanaged/orphan container", "name", m.name)
		if err := m.remove(ctx); err != nil {
			m.log.Error("container remove failed", "err", err)
			return m.errorReport("", err)
		}
		return client.AgentContainerObserved{ObservedState: client.ContainerAbsent}
	case ActionApply:
		m.log.Info("applying container spec", "name", m.name, "image", spec.Image, "hash", desiredHash)
		if err := m.apply(ctx, spec, desiredHash); err != nil {
			m.log.Error("container apply failed", "err", err)
			return m.errorReport(desiredHash, err)
		}
		obs, _ = m.observe(ctx)
		return m.report(hasSpec, desiredHash, obs, nil)
	}
	return m.report(hasSpec, desiredHash, obs, nil)
}

func (m *Manager) report(hasSpec bool, desiredHash string, obs Observed, applyErr error) client.AgentContainerObserved {
	if applyErr != nil {
		return m.errorReport(desiredHash, applyErr)
	}
	if !hasSpec || !obs.Exists {
		return client.AgentContainerObserved{ObservedState: client.ContainerAbsent}
	}
	state := client.ContainerRunning
	if !obs.Running {
		state = client.ContainerError
	}
	return client.AgentContainerObserved{ObservedState: state, SpecHash: obs.SpecHash}
}

func (m *Manager) errorReport(hash string, err error) client.AgentContainerObserved {
	msg := truncate(err.Error(), 1024)
	return client.AgentContainerObserved{ObservedState: client.ContainerError, SpecHash: hash, LastError: &msg}
}

func (m *Manager) observe(ctx context.Context) (Observed, error) {
	out, err := m.exec.Run(ctx, dockerTimeout, "docker", "inspect", m.name,
		"--format", "{{.State.Running}}|{{index .Config.Labels \""+labelSpecHash+"\"}}")
	if err != nil {
		if isNotFound(err) {
			return Observed{Exists: false}, nil
		}
		return Observed{Exists: false}, err
	}
	parts := strings.SplitN(strings.TrimSpace(out), "|", 2)
	obs := Observed{Exists: true}
	if len(parts) > 0 {
		obs.Running = strings.TrimSpace(parts[0]) == "true"
	}
	if len(parts) > 1 {
		hash := strings.TrimSpace(parts[1])
		if hash != "<no value>" {
			obs.SpecHash = hash
		}
	}
	return obs, nil
}

func (m *Manager) remove(ctx context.Context) error {
	_, err := m.exec.Run(ctx, dockerTimeout+5*time.Second, "docker", "rm", "-f", m.name)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

func (m *Manager) apply(ctx context.Context, spec *client.AgentContainerSpec, hash string) error {
	for _, v := range spec.Volumes {
		if v.Host == "" {
			continue
		}
		if err := os.MkdirAll(v.Host, 0o777); err != nil {
			return fmt.Errorf("mkdir volume %s: %w", v.Host, err)
		}
	}

	m.emit(ctx, client.AgentContainerObserved{ObservedState: client.ContainerPulling, SpecHash: hash})

	pullCtx, cancel := context.WithTimeout(ctx, pullTimeout)
	defer cancel()
	var lastLayers int
	var lastReport time.Time
	onProgress := func(pp PullProgress) {
		now := time.Now()
		if pp.LayersDone != lastLayers || now.Sub(lastReport) >= reportThrottle {
			lastLayers = pp.LayersDone
			lastReport = now
			pct := pp.Percent
			detail := fmt.Sprintf("%d/%d", pp.LayersDone, pp.LayersTotal)
			m.emit(ctx, client.AgentContainerObserved{
				ObservedState: client.ContainerPulling,
				Progress:      &pct,
				Detail:        &detail,
				SpecHash:      hash,
			})
		}
	}
	if err := m.puller.Pull(pullCtx, spec.Image, onProgress); err != nil {
		return fmt.Errorf("pull %s: %w", spec.Image, err)
	}

	if err := m.remove(ctx); err != nil {
		return fmt.Errorf("remove old: %w", err)
	}

	m.emit(ctx, client.AgentContainerObserved{ObservedState: client.ContainerStarting, SpecHash: hash})

	args := m.runArgs(spec, hash)
	if _, err := m.exec.Run(ctx, runTimeout, "docker", args...); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	m.emit(ctx, client.AgentContainerObserved{ObservedState: client.ContainerRunning, SpecHash: hash})
	return nil
}

func (m *Manager) runArgs(spec *client.AgentContainerSpec, hash string) []string {
	args := []string{
		"run", "-d",
		"--name", m.name,
		"--restart", "unless-stopped",
		"--network", "host",
		"--label", labelManaged + "=true",
		"--label", labelSpecHash + "=" + hash,
	}
	if spec.GPU {
		args = append(args, "--gpus", "all")
	}
	if spec.ShmSizeGB != nil && *spec.ShmSizeGB > 0 {
		args = append(args, "--shm-size="+strconv.FormatFloat(*spec.ShmSizeGB, 'g', -1, 64)+"g")
	} else {
		args = append(args, "--ipc=host")
	}
	for _, v := range spec.Volumes {
		if v.Host == "" || v.Container == "" {
			continue
		}
		args = append(args, "-v", v.Host+":"+v.Container+":rw")
	}
	for _, k := range sortedKeys(spec.Env) {
		args = append(args, "-e", k+"="+spec.Env[k])
	}
	args = append(args, spec.Image)
	if spec.RunCommand != nil && strings.TrimSpace(*spec.RunCommand) != "" {
		args = append(args, "sh", "-c", *spec.RunCommand)
	}
	return args
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, inspectNoExit) || strings.Contains(s, inspectNotFound)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"strconv"
	"sync"

	frpclient "github.com/fatedier/frp/client"
	v1 "github.com/fatedier/frp/pkg/config/v1"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

type Manager struct {
	log *slog.Logger

	mu     sync.Mutex
	hash   string
	cancel context.CancelFunc
}

func NewManager(log *slog.Logger) *Manager {
	return &Manager{log: log}
}

func (m *Manager) Reconcile(ctx context.Context, spec *client.AgentTunnelSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if spec == nil || len(spec.Proxies) == 0 {
		if m.cancel != nil {
			m.log.Info("tunnel stopping", "reason", "no spec")
			m.stopLocked()
		}
		return
	}

	desired := specHash(spec)
	if m.cancel != nil && desired == m.hash {
		return
	}
	if m.cancel != nil {
		m.log.Info("tunnel restarting", "reason", "spec changed")
		m.stopLocked()
	}
	if err := m.startLocked(ctx, spec, desired); err != nil {
		m.log.Error("tunnel start failed", "err", err)
	}
}

func (m *Manager) stopLocked() {
	m.cancel()
	m.cancel = nil
	m.hash = ""
}

func (m *Manager) startLocked(ctx context.Context, spec *client.AgentTunnelSpec, hash string) error {
	common, proxies, err := buildConfig(spec)
	if err != nil {
		return err
	}

	svc, err := frpclient.NewService(frpclient.ServiceOptions{Common: common, ProxyCfgs: proxies})
	if err != nil {
		return err
	}

	svcCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.hash = hash

	go func() {
		if runErr := svc.Run(svcCtx); runErr != nil && svcCtx.Err() == nil {
			m.log.Warn("tunnel service exited", "err", runErr)
		}
	}()

	m.log.Info("tunnel started", "frps", common.ServerAddr, "port", common.ServerPort, "proxies", len(proxies))
	return nil
}

func buildConfig(spec *client.AgentTunnelSpec) (*v1.ClientCommonConfig, []v1.ProxyConfigurer, error) {
	host, port, err := splitAddr(spec.FrpsAddr)
	if err != nil {
		return nil, nil, err
	}

	common := &v1.ClientCommonConfig{
		ServerAddr:    host,
		ServerPort:    port,
		LoginFailExit: boolPtr(false),
		Metadatas:     map[string]string{"slug": spec.Slug, "token": spec.FrpToken},
	}
	if err := common.Complete(); err != nil {
		return nil, nil, fmt.Errorf("complete common config: %w", err)
	}

	proxies := make([]v1.ProxyConfigurer, 0, len(spec.Proxies))
	for _, p := range spec.Proxies {
		cfg := &v1.HTTPProxyConfig{}
		cfg.Name = p.Subdomain
		cfg.Type = "http"
		cfg.LocalIP = "127.0.0.1"
		cfg.LocalPort = p.LocalPort
		cfg.SubDomain = p.Subdomain
		cfg.Complete("")
		proxies = append(proxies, cfg)
	}

	return common, proxies, nil
}

func splitAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("parse frps_addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("parse frps port %q: %w", portStr, err)
	}
	return host, port, nil
}

func specHash(spec *client.AgentTunnelSpec) string {
	raw, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return strconv.FormatUint(h.Sum64(), 16)
}

func boolPtr(b bool) *bool { return &b }

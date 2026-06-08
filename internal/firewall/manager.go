package firewall

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	sshPort       = 22
	sshProto      = "tcp"
	yougpuComment = "yougpu"
	ufwTimeout    = 10 * time.Second
)

var ruleRe = regexp.MustCompile(`(?m)^(\d+)/(tcp|udp)\b.*#\s*yougpu\b`)

type Port struct {
	Port     int
	Protocol string
}

type Manager struct {
	exec system.Executor
	log  *slog.Logger
}

func NewManager(exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{exec: exec, log: log}
}

func normalizeProto(p string) string {
	if strings.ToLower(strings.TrimSpace(p)) == "udp" {
		return "udp"
	}
	return "tcp"
}

func DesiredSet(spec *client.AgentFirewallSpec) map[Port]bool {
	out := map[Port]bool{{Port: sshPort, Protocol: sshProto}: true}
	if spec == nil {
		return out
	}
	for _, p := range spec.Ports {
		if p.Port > 0 && p.Port <= 65535 {
			out[Port{Port: p.Port, Protocol: normalizeProto(p.Protocol)}] = true
		}
	}
	return out
}

func Diff(desired, observed map[Port]bool) (toAdd, toRemove []Port) {
	for p := range desired {
		if !observed[p] {
			toAdd = append(toAdd, p)
		}
	}
	for p := range observed {
		if p.Port == sshPort && p.Protocol == sshProto {
			continue
		}
		if !desired[p] {
			toRemove = append(toRemove, p)
		}
	}
	return toAdd, toRemove
}

func ParseYougpuRules(status string) map[Port]bool {
	out := map[Port]bool{}
	for _, match := range ruleRe.FindAllStringSubmatch(status, -1) {
		port, err := strconv.Atoi(match[1])
		if err != nil {
			continue
		}
		out[Port{Port: port, Protocol: match[2]}] = true
	}
	return out
}

func (m *Manager) Reconcile(ctx context.Context, spec *client.AgentFirewallSpec) client.AgentFirewallObserved {
	if spec == nil {
		return client.AgentFirewallObserved{ObservedState: client.FirewallApplied}
	}

	status, err := m.status(ctx)
	if err != nil {
		return m.errReport(err)
	}
	if strings.Contains(status, "Status: inactive") {
		if _, err := m.exec.Run(ctx, ufwTimeout, "ufw", "--force", "enable"); err != nil {
			return m.errReport(err)
		}
		status, err = m.status(ctx)
		if err != nil {
			return m.errReport(err)
		}
	}

	observed := ParseYougpuRules(status)
	desired := DesiredSet(spec)
	toAdd, toRemove := Diff(desired, observed)

	var errs []string
	for _, p := range toAdd {
		m.log.Info("opening firewall port", "port", p.Port, "proto", p.Protocol)
		if err := m.allow(ctx, p); err != nil {
			m.log.Error("ufw allow failed", "port", p.Port, "err", err)
			errs = append(errs, err.Error())
		}
	}
	for _, p := range toRemove {
		m.log.Info("closing firewall port", "port", p.Port, "proto", p.Protocol)
		if err := m.delete(ctx, p); err != nil {
			m.log.Error("ufw delete failed", "port", p.Port, "err", err)
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		msg := truncate(strings.Join(errs, "; "), 1024)
		return client.AgentFirewallObserved{ObservedState: client.FirewallError, LastError: &msg}
	}
	return client.AgentFirewallObserved{ObservedState: client.FirewallApplied}
}

func (m *Manager) status(ctx context.Context) (string, error) {
	return m.exec.Run(ctx, ufwTimeout, "ufw", "status")
}

func (m *Manager) allow(ctx context.Context, p Port) error {
	_, err := m.exec.Run(ctx, ufwTimeout, "ufw", "allow", fmt.Sprintf("%d/%s", p.Port, p.Protocol), "comment", yougpuComment)
	return err
}

func (m *Manager) delete(ctx context.Context, p Port) error {
	_, err := m.exec.Run(ctx, ufwTimeout, "ufw", "delete", "allow", fmt.Sprintf("%d/%s", p.Port, p.Protocol))
	return err
}

func (m *Manager) errReport(err error) client.AgentFirewallObserved {
	msg := truncate(err.Error(), 1024)
	return client.AgentFirewallObserved{ObservedState: client.FirewallError, LastError: &msg}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

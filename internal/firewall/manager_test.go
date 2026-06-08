package firewall

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestDesiredSetAlwaysIncludesSSH(t *testing.T) {
	d := DesiredSet(nil)
	if !d[Port{Port: 22, Protocol: "tcp"}] {
		t.Fatal("22/tcp must always be desired (anti-lockout)")
	}
}

func TestDesiredSetNormalizesProtoAndDropsInvalid(t *testing.T) {
	d := DesiredSet(&client.AgentFirewallSpec{Ports: []client.FirewallPort{
		{Port: 8888, Protocol: "TCP"},
		{Port: 9000, Protocol: "udp"},
		{Port: 70000, Protocol: "tcp"},
		{Port: 0, Protocol: "tcp"},
	}})
	if !d[Port{Port: 8888, Protocol: "tcp"}] {
		t.Error("TCP must normalize to tcp")
	}
	if !d[Port{Port: 9000, Protocol: "udp"}] {
		t.Error("udp port missing")
	}
	if d[Port{Port: 70000, Protocol: "tcp"}] || d[Port{Port: 0, Protocol: "tcp"}] {
		t.Error("out-of-range ports must be dropped")
	}
}

func TestDiffNeverRemovesSSH(t *testing.T) {
	desired := map[Port]bool{{Port: 80, Protocol: "tcp"}: true}
	observed := map[Port]bool{{Port: 22, Protocol: "tcp"}: true, {Port: 8888, Protocol: "tcp"}: true}
	toAdd, toRemove := Diff(desired, observed)

	addHas := func(p Port) bool {
		for _, x := range toAdd {
			if x == p {
				return true
			}
		}
		return false
	}
	removeHas := func(p Port) bool {
		for _, x := range toRemove {
			if x == p {
				return true
			}
		}
		return false
	}

	if !addHas(Port{Port: 80, Protocol: "tcp"}) {
		t.Error("80/tcp must be added")
	}
	if !removeHas(Port{Port: 8888, Protocol: "tcp"}) {
		t.Error("orphan 8888/tcp must be removed")
	}
	if removeHas(Port{Port: 22, Protocol: "tcp"}) {
		t.Error("22/tcp must NEVER be removed even if not in desired")
	}
}

func TestParseYougpuRulesOnlyTaggedAndDedupV6(t *testing.T) {
	status := `Status: active

To                         Action      From
--                         ------      ----
22/tcp                     ALLOW       Anywhere                   # yougpu
8888/tcp                   ALLOW       Anywhere                   # yougpu
9000/udp                   ALLOW       Anywhere                   # yougpu
5000/tcp                   ALLOW       Anywhere                   # manual user rule
22/tcp (v6)                ALLOW       Anywhere (v6)              # yougpu
`
	rules := ParseYougpuRules(status)
	if !rules[Port{Port: 8888, Protocol: "tcp"}] || !rules[Port{Port: 9000, Protocol: "udp"}] {
		t.Error("tagged rules must be parsed")
	}
	if rules[Port{Port: 5000, Protocol: "tcp"}] {
		t.Error("untagged user rule must NOT be managed")
	}
	if len(rules) != 3 {
		t.Errorf("v6 duplicate must dedup; expected 3 got %d", len(rules))
	}
}

type scriptExec struct {
	statusOut string
	calls     *[]string
}

func (s scriptExec) Run(_ context.Context, _ time.Duration, name string, args ...string) (string, error) {
	if name == "ufw" && len(args) > 0 && args[0] == "status" {
		return s.statusOut, nil
	}
	if s.calls != nil {
		*s.calls = append(*s.calls, name+" "+strings.Join(args, " "))
	}
	return "", nil
}

func TestReconcileAddsMissingAndRemovesOrphan(t *testing.T) {
	status := `Status: active

To                         Action      From
--                         ------      ----
22/tcp                     ALLOW       Anywhere                   # yougpu
7777/tcp                   ALLOW       Anywhere                   # yougpu
`
	var calls []string
	m := NewManager(scriptExec{statusOut: status, calls: &calls}, testLogger())

	obs := m.Reconcile(context.Background(), &client.AgentFirewallSpec{Ports: []client.FirewallPort{
		{Port: 22, Protocol: "tcp"},
		{Port: 8888, Protocol: "tcp"},
	}})

	joined := strings.Join(calls, " | ")
	if !strings.Contains(joined, "ufw allow 8888/tcp comment yougpu") {
		t.Errorf("must open 8888/tcp, calls: %s", joined)
	}
	if !strings.Contains(joined, "ufw delete allow 7777/tcp") {
		t.Errorf("must remove orphan 7777/tcp, calls: %s", joined)
	}
	if strings.Contains(joined, "delete allow 22/tcp") {
		t.Errorf("must NEVER remove 22/tcp, calls: %s", joined)
	}
	if obs.ObservedState != client.FirewallApplied {
		t.Errorf("expected applied, got %s", obs.ObservedState)
	}
}

func TestReconcileEnablesWhenInactive(t *testing.T) {
	var calls []string
	m := NewManager(scriptExec{statusOut: "Status: inactive", calls: &calls}, testLogger())
	m.Reconcile(context.Background(), &client.AgentFirewallSpec{Ports: nil})
	if !strings.Contains(strings.Join(calls, " | "), "ufw --force enable") {
		t.Errorf("must enable ufw when inactive, calls: %v", calls)
	}
}

func TestReconcileNilSpecNoop(t *testing.T) {
	var calls []string
	m := NewManager(scriptExec{statusOut: "Status: active", calls: &calls}, testLogger())
	obs := m.Reconcile(context.Background(), nil)
	if len(calls) != 0 {
		t.Errorf("nil spec must not touch ufw, calls: %v", calls)
	}
	if obs.ObservedState != client.FirewallApplied {
		t.Errorf("nil spec → applied, got %s", obs.ObservedState)
	}
}

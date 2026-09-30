package client

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAgentSetupObservedAlwaysHasState(t *testing.T) {
	raw, err := json.Marshal(AgentSetupObserved{ObservedState: SetupInstallingDocker})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"observed_state":"installing_docker"`) {
		t.Errorf("observed_state must always be present, got %s", raw)
	}
}

func TestAgentSetupObservedOmitsEmptyOptionals(t *testing.T) {
	raw, err := json.Marshal(AgentSetupObserved{ObservedState: SetupReady})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(raw)
	for _, k := range []string{"progress", "detail", "last_error", "last_log"} {
		if strings.Contains(s, k) {
			t.Errorf("empty %q must be omitted (nil→null breaks backend Zod), got %s", k, s)
		}
	}
}

func TestAgentSetupObservedSerializesOptionals(t *testing.T) {
	p := 40
	detail := "Docker"
	lastErr := "apt failed"
	lastLog := "--- error ---\napt failed"
	raw, err := json.Marshal(AgentSetupObserved{
		ObservedState: SetupError,
		Progress:      &p,
		Detail:        &detail,
		LastError:     &lastErr,
		LastLog:       &lastLog,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(raw)
	for _, want := range []string{`"progress":40`, `"detail":"Docker"`, `"last_error":"apt failed"`, `"last_log":`} {
		if !strings.Contains(s, want) {
			t.Errorf("expected %s in %s", want, s)
		}
	}
}

func TestAgentStatusOmitsSetupWhenNil(t *testing.T) {
	raw, err := json.Marshal(AgentStatus{ObservedGeneration: 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "setup") {
		t.Errorf("nil setup must be omitted, got %s", raw)
	}
}

func TestLifecycleErrorCarriesReason(t *testing.T) {
	reason := "выгрузка кэша дисков не движется"
	raw, err := json.Marshal(AgentStatus{Lifecycle: StatusLifecycle{ObservedState: "error", LastError: &reason}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"lifecycle":{"observed_state":"error","last_error":"выгрузка кэша дисков не движется"}`) {
		t.Fatalf("got %s", raw)
	}
	raw, _ = json.Marshal(AgentStatus{Lifecycle: StatusLifecycle{ObservedState: "alive"}})
	if strings.Contains(string(raw), "last_error") {
		t.Fatalf("empty last_error must be omitted, got %s", raw)
	}
}

func TestTunnelStatusIsOptional(t *testing.T) {
	raw, _ := json.Marshal(AgentStatus{})
	if strings.Contains(string(raw), "tunnel") {
		t.Fatalf("status without a tunnel must not mention it, got %s", raw)
	}
	lost := "нет связи со шлюзом"
	raw, _ = json.Marshal(AgentStatus{Tunnel: &AgentTunnelObserved{ObservedState: TunnelDisconnected, LastError: &lost}})
	if !strings.Contains(string(raw), `"tunnel":{"observed_state":"disconnected","last_error":"нет связи со шлюзом"}`) {
		t.Fatalf("got %s", raw)
	}
}

func TestStateSpecIgnoresFreezeCommand(t *testing.T) {
	var spec AgentStateSpec
	if err := json.Unmarshal([]byte(`{"include":["user"],"freeze_command":["yougpu-freeze"],"pending":false}`), &spec); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(spec)
	if strings.Contains(string(raw), "freeze") {
		t.Fatalf("freeze_command is gone from the contract, got %s", raw)
	}
}

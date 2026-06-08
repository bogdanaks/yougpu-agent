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

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTokenFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	return tokenPath
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Setenv("AGENT_BACKEND_URL", "http://localhost:5005")
	t.Setenv("AGENT_TOKEN_FILE", writeTokenFile(t))
	t.Setenv("AGENT_STATE_DIR", t.TempDir())
	t.Setenv("AGENT_HEARTBEAT_INTERVAL", "")
	t.Setenv("AGENT_STS_ROTATE_INTERVAL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HeartbeatInterval != defaultHeartbeat {
		t.Errorf("heartbeat = %s, want default %s", cfg.HeartbeatInterval, defaultHeartbeat)
	}
	if cfg.StsRotateInterval != defaultStsRotate {
		t.Errorf("sts rotate = %s, want default %s", cfg.StsRotateInterval, defaultStsRotate)
	}
}

func TestLoadParsesEnvDurations(t *testing.T) {
	t.Setenv("AGENT_BACKEND_URL", "http://localhost:5005")
	t.Setenv("AGENT_TOKEN_FILE", writeTokenFile(t))
	t.Setenv("AGENT_STATE_DIR", t.TempDir())
	t.Setenv("AGENT_HEARTBEAT_INTERVAL", "2s")
	t.Setenv("AGENT_STS_ROTATE_INTERVAL", "5m")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HeartbeatInterval != 2*time.Second {
		t.Errorf("heartbeat = %s, want 2s", cfg.HeartbeatInterval)
	}
	if cfg.StsRotateInterval != 5*time.Minute {
		t.Errorf("sts rotate = %s, want 5m", cfg.StsRotateInterval)
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv("AGENT_BACKEND_URL", "http://localhost:5005")
	t.Setenv("AGENT_TOKEN_FILE", writeTokenFile(t))
	t.Setenv("AGENT_STATE_DIR", t.TempDir())
	t.Setenv("AGENT_HEARTBEAT_INTERVAL", "abc")

	if _, err := Load(); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestLoadRejectsZeroDuration(t *testing.T) {
	t.Setenv("AGENT_BACKEND_URL", "http://localhost:5005")
	t.Setenv("AGENT_TOKEN_FILE", writeTokenFile(t))
	t.Setenv("AGENT_STATE_DIR", t.TempDir())
	t.Setenv("AGENT_HEARTBEAT_INTERVAL", "0s")

	if _, err := Load(); err == nil {
		t.Fatal("expected positive-duration error, got nil")
	}
}

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
	t.Setenv("AGENT_CREDS_REFRESH_THRESHOLD", "")
	t.Setenv("AGENT_CREDS_PERIODIC_INTERVAL", "")
	t.Setenv("AGENT_RECONCILE_INTERVAL", "")
	t.Setenv("AGENT_RCLONE_RC_PORT_BASE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HeartbeatInterval != defaultHeartbeat {
		t.Errorf("heartbeat = %s, want default %s", cfg.HeartbeatInterval, defaultHeartbeat)
	}
	if cfg.CredsRefreshThreshold != defaultCredsRefreshThr {
		t.Errorf("creds refresh threshold = %s, want default %s", cfg.CredsRefreshThreshold, defaultCredsRefreshThr)
	}
	if cfg.CredsPeriodicInterval != defaultCredsPeriodic {
		t.Errorf("creds periodic = %s, want default %s", cfg.CredsPeriodicInterval, defaultCredsPeriodic)
	}
	if cfg.ReconcileInterval != defaultReconcile {
		t.Errorf("reconcile = %s, want default %s", cfg.ReconcileInterval, defaultReconcile)
	}
	if cfg.RcloneRcPortBase != defaultRcloneRcPortBase {
		t.Errorf("rc port base = %d, want default %d", cfg.RcloneRcPortBase, defaultRcloneRcPortBase)
	}
}

func TestLoadParsesEnvDurations(t *testing.T) {
	t.Setenv("AGENT_BACKEND_URL", "http://localhost:5005")
	t.Setenv("AGENT_TOKEN_FILE", writeTokenFile(t))
	t.Setenv("AGENT_STATE_DIR", t.TempDir())
	t.Setenv("AGENT_HEARTBEAT_INTERVAL", "2s")
	t.Setenv("AGENT_CREDS_REFRESH_THRESHOLD", "5m")
	t.Setenv("AGENT_CREDS_PERIODIC_INTERVAL", "30s")
	t.Setenv("AGENT_RECONCILE_INTERVAL", "10s")
	t.Setenv("AGENT_RCLONE_RC_PORT_BASE", "6000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HeartbeatInterval != 2*time.Second {
		t.Errorf("heartbeat = %s, want 2s", cfg.HeartbeatInterval)
	}
	if cfg.CredsRefreshThreshold != 5*time.Minute {
		t.Errorf("creds refresh threshold = %s, want 5m", cfg.CredsRefreshThreshold)
	}
	if cfg.CredsPeriodicInterval != 30*time.Second {
		t.Errorf("creds periodic = %s, want 30s", cfg.CredsPeriodicInterval)
	}
	if cfg.ReconcileInterval != 10*time.Second {
		t.Errorf("reconcile = %s, want 10s", cfg.ReconcileInterval)
	}
	if cfg.RcloneRcPortBase != 6000 {
		t.Errorf("rc port base = %d, want 6000", cfg.RcloneRcPortBase)
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

func TestLoadRejectsRcPortOutOfRange(t *testing.T) {
	t.Setenv("AGENT_BACKEND_URL", "http://localhost:5005")
	t.Setenv("AGENT_TOKEN_FILE", writeTokenFile(t))
	t.Setenv("AGENT_STATE_DIR", t.TempDir())
	t.Setenv("AGENT_RCLONE_RC_PORT_BASE", "100")

	if _, err := Load(); err == nil {
		t.Fatal("expected range error for rc port base, got nil")
	}
}

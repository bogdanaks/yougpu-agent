package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	BackendURL            string
	Token                 string
	StateDir              string
	HeartbeatInterval     time.Duration
	CredsRefreshThreshold time.Duration
	CredsPeriodicInterval time.Duration
	ReconcileInterval     time.Duration
	RcloneRcPortBase      int
	DiskDriver            string
	LogLevel              string
}

const (
	envBackendURL            = "AGENT_BACKEND_URL"
	envTokenFile             = "AGENT_TOKEN_FILE"
	envStateDir              = "AGENT_STATE_DIR"
	envHeartbeatInterval     = "AGENT_HEARTBEAT_INTERVAL"
	envCredsRefreshThreshold = "AGENT_CREDS_REFRESH_THRESHOLD"
	envCredsPeriodicInterval = "AGENT_CREDS_PERIODIC_INTERVAL"
	envReconcileInterval     = "AGENT_RECONCILE_INTERVAL"
	envRcloneRcPortBase      = "AGENT_RCLONE_RC_PORT_BASE"
	envDiskDriver            = "AGENT_DISK_DRIVER"
	envLogLevel              = "AGENT_LOG_LEVEL"
	defaultTokenFile         = "/var/lib/agent/token"
	defaultStateDir          = "/var/lib/agent"
	defaultHeartbeat         = 30 * time.Second
	defaultCredsRefreshThr   = 3 * time.Hour
	defaultCredsPeriodic     = 1 * time.Hour
	defaultReconcile         = 60 * time.Second
	defaultRcloneRcPortBase  = 5572
	defaultLogLevel          = "info"
	DiskDriverSystemd        = "systemd"
	DiskDriverDirect         = "direct"
)

func Load() (*Config, error) {
	backendURL := strings.TrimRight(os.Getenv(envBackendURL), "/")
	if backendURL == "" {
		return nil, fmt.Errorf("%s is required", envBackendURL)
	}

	tokenFile := os.Getenv(envTokenFile)
	if tokenFile == "" {
		tokenFile = defaultTokenFile
	}
	token, err := readToken(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token from %s: %w", tokenFile, err)
	}

	stateDir := os.Getenv(envStateDir)
	if stateDir == "" {
		stateDir = defaultStateDir
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir %s: %w", stateDir, err)
	}

	heartbeat, err := durationEnv(envHeartbeatInterval, defaultHeartbeat)
	if err != nil {
		return nil, err
	}
	credsRefreshThr, err := durationEnv(envCredsRefreshThreshold, defaultCredsRefreshThr)
	if err != nil {
		return nil, err
	}
	credsPeriodic, err := durationEnv(envCredsPeriodicInterval, defaultCredsPeriodic)
	if err != nil {
		return nil, err
	}
	reconcile, err := durationEnv(envReconcileInterval, defaultReconcile)
	if err != nil {
		return nil, err
	}
	rcPortBase, err := intEnv(envRcloneRcPortBase, defaultRcloneRcPortBase)
	if err != nil {
		return nil, err
	}
	if rcPortBase < 1024 || rcPortBase > 65000 {
		return nil, fmt.Errorf("%s must be between 1024 and 65000, got %d", envRcloneRcPortBase, rcPortBase)
	}

	diskDriver := strings.TrimSpace(os.Getenv(envDiskDriver))
	if diskDriver == "" {
		diskDriver = DiskDriverSystemd
	}
	if diskDriver != DiskDriverSystemd && diskDriver != DiskDriverDirect {
		return nil, fmt.Errorf("%s must be %q or %q, got %q", envDiskDriver, DiskDriverSystemd, DiskDriverDirect, diskDriver)
	}

	logLevel := strings.ToLower(strings.TrimSpace(os.Getenv(envLogLevel)))
	if logLevel == "" {
		logLevel = defaultLogLevel
	}
	if logLevel != "debug" && logLevel != "info" && logLevel != "warn" && logLevel != "error" {
		return nil, fmt.Errorf("%s must be one of debug|info|warn|error, got %q", envLogLevel, logLevel)
	}

	return &Config{
		BackendURL:            backendURL,
		Token:                 token,
		StateDir:              stateDir,
		HeartbeatInterval:     heartbeat,
		CredsRefreshThreshold: credsRefreshThr,
		CredsPeriodicInterval: credsPeriodic,
		ReconcileInterval:     reconcile,
		RcloneRcPortBase:      rcPortBase,
		DiskDriver:            diskDriver,
		LogLevel:              logLevel,
	}, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s=%q: %w", name, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", name, d)
	}
	return d, nil
}

func intEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	var v int
	if _, err := fmt.Sscanf(raw, "%d", &v); err != nil {
		return 0, fmt.Errorf("parse %s=%q: %w", name, raw, err)
	}
	return v, nil
}

func readToken(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", errors.New("token file is empty")
	}
	return token, nil
}

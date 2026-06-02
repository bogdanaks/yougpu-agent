package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	BackendURL        string
	Token             string
	StateDir          string
	HeartbeatInterval time.Duration
	StsRotateInterval time.Duration
	// DiskDriver — "systemd" (prod, default) или "direct" (тесты в контейнере без systemd:
	// rclone запускается как --daemon процесс, mount tracking через файлы в StateDir).
	DiskDriver string
}

const (
	envBackendURL        = "AGENT_BACKEND_URL"
	envTokenFile         = "AGENT_TOKEN_FILE"
	envStateDir          = "AGENT_STATE_DIR"
	envHeartbeatInterval = "AGENT_HEARTBEAT_INTERVAL"
	envStsRotateInterval = "AGENT_STS_ROTATE_INTERVAL"
	envDiskDriver        = "AGENT_DISK_DRIVER"
	defaultTokenFile     = "/var/lib/agent/token"
	defaultStateDir      = "/var/lib/agent"
	defaultHeartbeat     = 30 * time.Second
	defaultStsRotate     = 12 * time.Hour
	DiskDriverSystemd    = "systemd"
	DiskDriverDirect     = "direct"
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
	stsRotate, err := durationEnv(envStsRotateInterval, defaultStsRotate)
	if err != nil {
		return nil, err
	}

	diskDriver := strings.TrimSpace(os.Getenv(envDiskDriver))
	if diskDriver == "" {
		diskDriver = DiskDriverSystemd
	}
	if diskDriver != DiskDriverSystemd && diskDriver != DiskDriverDirect {
		return nil, fmt.Errorf("%s must be %q or %q, got %q", envDiskDriver, DiskDriverSystemd, DiskDriverDirect, diskDriver)
	}

	return &Config{
		BackendURL:        backendURL,
		Token:             token,
		StateDir:          stateDir,
		HeartbeatInterval: heartbeat,
		StsRotateInterval: stsRotate,
		DiskDriver:        diskDriver,
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

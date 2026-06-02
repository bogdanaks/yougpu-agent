package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	BackendURL string
	Token      string
	StateDir   string
}

const (
	envBackendURL    = "AGENT_BACKEND_URL"
	envTokenFile     = "AGENT_TOKEN_FILE"
	envStateDir      = "AGENT_STATE_DIR"
	defaultTokenFile = "/var/lib/agent/token"
	defaultStateDir  = "/var/lib/agent"
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

	return &Config{
		BackendURL: backendURL,
		Token:      token,
		StateDir:   stateDir,
	}, nil
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

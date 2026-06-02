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
	defaultTokenFile = "/var/lib/yougpu/provisioning_token"
	defaultStateDir  = "/var/lib/yougpu"
)

func Load() (*Config, error) {
	backendURL := strings.TrimRight(os.Getenv("YOUGPU_BACKEND_URL"), "/")
	if backendURL == "" {
		return nil, errors.New("YOUGPU_BACKEND_URL is required")
	}

	tokenFile := os.Getenv("YOUGPU_TOKEN_FILE")
	if tokenFile == "" {
		tokenFile = defaultTokenFile
	}
	token, err := readToken(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token from %s: %w", tokenFile, err)
	}

	stateDir := os.Getenv("YOUGPU_STATE_DIR")
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

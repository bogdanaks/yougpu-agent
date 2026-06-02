package sts

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	defaultRcloneConfigPath = "/root/.config/rclone/rclone.conf"
	configFileMode          = 0o600
	rcloneRemote            = "remote"
)

type Restarter interface {
	RestartAll(ctx context.Context) error
}

type Rotator struct {
	client     *client.Client
	exec       system.Executor
	log        *slog.Logger
	interval   time.Duration
	configPath string
}

func NewRotator(c *client.Client, exec system.Executor, log *slog.Logger, interval time.Duration) *Rotator {
	return &Rotator{
		client:     c,
		exec:       exec,
		log:        log,
		interval:   interval,
		configPath: defaultRcloneConfigPath,
	}
}

func (r *Rotator) SetConfigPath(p string) { r.configPath = p }

func (r *Rotator) RotateOnce(ctx context.Context, restarter Restarter) error {
	creds, err := r.client.RotateStorageKeys(ctx)
	if err != nil {
		return fmt.Errorf("rotate request: %w", err)
	}
	if err := r.writeConfig(creds); err != nil {
		return fmt.Errorf("write rclone config: %w", err)
	}
	if restarter != nil {
		if err := restarter.RestartAll(ctx); err != nil {
			return fmt.Errorf("restart mounts: %w", err)
		}
	}
	return nil
}

// Run blocks until ctx is cancelled. First rotation runs immediately (initial creds),
// subsequent ones on a ticker.
func (r *Rotator) Run(ctx context.Context, restarter Restarter) {
	if err := r.RotateOnce(ctx, nil); err != nil {
		r.log.Error("initial rotation failed", "err", err)
	}

	t := time.NewTicker(r.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.RotateOnce(ctx, restarter); err != nil {
				r.log.Error("rotation failed", "err", err)
			} else {
				r.log.Info("credentials rotated")
			}
		}
	}
}

func (r *Rotator) writeConfig(c *client.RotateStorageKeysResponse) error {
	dir := filepath.Dir(r.configPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	body := fmt.Sprintf(`[%s]
type = s3
provider = Minio
env_auth = false
access_key_id = %s
secret_access_key = %s
session_token = %s
endpoint = %s
acl = private
`, rcloneRemote, c.AccessKey, c.SecretKey, c.SessionToken, c.Endpoint)

	tmp := r.configPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), configFileMode); err != nil {
		return err
	}
	return os.Rename(tmp, r.configPath)
}

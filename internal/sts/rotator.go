// Package sts периодически рефрешит S3 credentials у backend'а (12h-сессии MinIO STS)
// и переписывает rclone.conf. После переписывания зовёт restartAll, чтобы все mount'ы
// подхватили новые ключи без потери данных.
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
	rcloneRemote            = "yougpu-r2"
)

// Restarter — то, что Rotator зовёт после обновления creds: рестарт всех rclone-mount'ов.
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

// RotateOnce выполняет один цикл ротации. Используется при старте (initial creds) и
// при ошибке Auth от S3 (caller сам решит когда дёрнуть).
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

// Run запускает периодическую ротацию. Блокируется до отмены ctx.
// Первый цикл выполняется немедленно (initial creds), дальше — по тикеру.
func (r *Rotator) Run(ctx context.Context, restarter Restarter) {
	if err := r.RotateOnce(ctx, nil); err != nil {
		// At boot there are usually no mounts yet — restarter==nil to avoid no-op churn.
		r.log.Error("initial sts rotation failed", "err", err)
	}

	t := time.NewTicker(r.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.RotateOnce(ctx, restarter); err != nil {
				r.log.Error("sts rotation failed", "err", err)
			} else {
				r.log.Info("sts credentials rotated")
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

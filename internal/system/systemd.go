package system

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

const (
	defaultSystemctlTimeout = 60 * time.Second
	stopTimeout             = 30 * time.Minute
)

type Systemd interface {
	DaemonReload(ctx context.Context) error
	Enable(ctx context.Context, unit string) error
	Disable(ctx context.Context, unit string) error
	Start(ctx context.Context, unit string) error
	Stop(ctx context.Context, unit string) error
	Restart(ctx context.Context, unit string) error
	IsActive(ctx context.Context, unit string) (bool, error)
	Poweroff(ctx context.Context) error
}

type Systemctl struct {
	exec Executor
	log  *slog.Logger
}

func NewSystemd(exec Executor, log *slog.Logger) *Systemctl {
	return &Systemctl{exec: exec, log: log}
}

func (s *Systemctl) DaemonReload(ctx context.Context) error {
	_, err := s.exec.Run(ctx, defaultSystemctlTimeout, "systemctl", "daemon-reload")
	return err
}

func (s *Systemctl) Enable(ctx context.Context, unit string) error {
	_, err := s.exec.Run(ctx, defaultSystemctlTimeout, "systemctl", "enable", unit)
	return err
}

func (s *Systemctl) Disable(ctx context.Context, unit string) error {
	_, err := s.exec.Run(ctx, defaultSystemctlTimeout, "systemctl", "disable", unit)
	return err
}

func (s *Systemctl) Start(ctx context.Context, unit string) error {
	_, err := s.exec.Run(ctx, defaultSystemctlTimeout, "systemctl", "start", unit)
	return err
}

func (s *Systemctl) Stop(ctx context.Context, unit string) error {
	_, err := s.exec.Run(ctx, stopTimeout, "systemctl", "stop", unit)
	return err
}

func (s *Systemctl) Restart(ctx context.Context, unit string) error {
	_, err := s.exec.Run(ctx, stopTimeout, "systemctl", "restart", unit)
	return err
}

// IsActive treats a non-active exit code as `false` rather than an error — `systemctl is-active`
// exits non-zero whenever the unit is not active, even for normal states like "inactive".
func (s *Systemctl) IsActive(ctx context.Context, unit string) (bool, error) {
	out, err := s.exec.Run(ctx, defaultSystemctlTimeout, "systemctl", "is-active", unit)
	state := strings.TrimSpace(out)
	if state == "active" {
		return true, nil
	}
	if err != nil && state != "" {
		return false, nil
	}
	return false, err
}

func (s *Systemctl) Poweroff(ctx context.Context) error {
	_, err := s.exec.Run(ctx, defaultSystemctlTimeout, "systemctl", "poweroff")
	return err
}

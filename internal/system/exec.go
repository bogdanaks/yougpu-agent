package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"time"
)

// Executor runs external commands. Mockable for tests.
type Executor interface {
	Run(ctx context.Context, timeout time.Duration, name string, args ...string) (stdout string, err error)
}

type CmdExecutor struct {
	log *slog.Logger
}

func NewExecutor(log *slog.Logger) *CmdExecutor {
	return &CmdExecutor{log: log}
}

func (e *CmdExecutor) Run(ctx context.Context, timeout time.Duration, name string, args ...string) (string, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	e.log.Debug("exec", "cmd", name, "args", args)
	err := cmd.Run()
	if err != nil {
		return stdout.String(), fmt.Errorf("%s %v: %w (stderr: %s)", name, args, err, truncate(stderr.String(), 512))
	}
	return stdout.String(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}

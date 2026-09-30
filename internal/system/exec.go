package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/fetch"
)

const maxStderr = 512

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

	shown := withoutEnvValues(args)
	e.log.Debug("exec", "cmd", name, "args", shown)
	err := cmd.Run()
	if err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w (stderr: %s)", name, strings.Join(shown, " "), err, fetch.Clip(stderr.String(), maxStderr))
	}
	return stdout.String(), nil
}

func withoutEnvValues(args []string) []string {
	shown := make([]string, len(args))
	for i, arg := range args {
		shown[i] = arg
		if i > 0 && (args[i-1] == "-e" || args[i-1] == "--env") {
			shown[i], _, _ = strings.Cut(arg, "=")
		}
	}
	return shown
}

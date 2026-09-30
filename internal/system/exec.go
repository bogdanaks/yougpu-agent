package system

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/fetch"
)

const (
	maxStderr        = 512
	defaultWaitDelay = 10 * time.Second
)

type Executor interface {
	Run(ctx context.Context, timeout time.Duration, name string, args ...string) (stdout string, err error)
}

type CmdExecutor struct {
	log       *slog.Logger
	waitDelay time.Duration
}

func NewExecutor(log *slog.Logger) *CmdExecutor {
	return &CmdExecutor{log: log, waitDelay: defaultWaitDelay}
}

func Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = defaultWaitDelay
	return cmd
}

func (e *CmdExecutor) Run(ctx context.Context, timeout time.Duration, name string, args ...string) (string, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var stdout, stderr bytes.Buffer
	cmd := Command(ctx, name, args...)
	cmd.WaitDelay = e.waitDelay
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	shown := withoutEnvValues(args)
	e.log.Debug("exec", "cmd", name, "args", shown)
	err := cmd.Run()
	if errors.Is(err, exec.ErrWaitDelay) {
		e.log.Debug("exec left processes holding its output", "cmd", name)
		err = nil
	}
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

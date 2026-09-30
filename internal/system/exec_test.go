package system

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestFailedCommandHidesEnvValues(t *testing.T) {
	var logs bytes.Buffer
	e := NewExecutor(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))

	_, err := e.Run(context.Background(), 0, "sh", "-c", "exit 3", "-e", "JUPYTER_TOKEN=hunter2", "--env", "HF_TOKEN=hf_secret", "image")

	if err == nil {
		t.Fatal("want an error")
	}
	for _, secret := range []string{"hunter2", "hf_secret"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log leaks %q: %s", secret, logs.String())
		}
	}
	for _, want := range []string{"-e JUPYTER_TOKEN", "--env HF_TOKEN", "image"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error must still name the command, missing %q: %v", want, err)
		}
	}
}

func quietExecutor() *CmdExecutor {
	return NewExecutor(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestTimeoutKillsGrandchildren(t *testing.T) {
	start := time.Now()
	_, err := quietExecutor().Run(context.Background(), 200*time.Millisecond, "sh", "-c", "sleep 30 & wait")

	if err == nil {
		t.Fatal("timed out command must fail")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Run waited %s for a grandchild after the timeout", took)
	}
}

func TestCancelKillsGrandchildren(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	_, err := quietExecutor().Run(ctx, 0, "sh", "-c", "sleep 30 | cat")

	if err == nil {
		t.Fatal("cancelled command must fail")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Run waited %s for the pipeline after cancel", took)
	}
}

func TestLeftoverChildDoesNotHoldRun(t *testing.T) {
	e := quietExecutor()
	e.waitDelay = 200 * time.Millisecond
	start := time.Now()
	out, err := e.Run(context.Background(), 0, "sh", "-c", "echo done; sleep 5 &")

	if err != nil {
		t.Fatalf("command itself succeeded: %v", err)
	}
	if strings.TrimSpace(out) != "done" {
		t.Fatalf("stdout %q", out)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Run waited %s for a child that outlived the command", took)
	}
}

func TestCommandKillsItsProcessGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Command(ctx, "sh", "-c", "sleep 30 & wait").CombinedOutput()

	if err == nil {
		t.Fatal("timed out command must fail")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("command waited %s for a grandchild after the timeout", took)
	}
}

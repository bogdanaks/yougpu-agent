package system

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
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

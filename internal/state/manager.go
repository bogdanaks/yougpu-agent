package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/content"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	restoredMarker = "state_restored"
	savedMarker    = "state_saved"
	maxUnpacked    = 20 << 30
	maxUpload      = 5 << 30
	attempts       = 3
	freezeTimeout  = 2 * time.Minute
)

type Manager struct {
	stateDir   string
	exec       system.Executor
	http       *http.Client
	log        *slog.Logger
	reporter   func(context.Context, client.AgentStateObserved)
	retryDelay time.Duration
}

func New(stateDir string, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{stateDir: stateDir, exec: exec, http: &http.Client{}, log: log, retryDelay: 5 * time.Second}
}

func (m *Manager) SetReporter(f func(context.Context, client.AgentStateObserved)) {
	m.reporter = f
}

func (m *Manager) report(ctx context.Context, obs client.AgentStateObserved) *client.AgentStateObserved {
	if m.reporter != nil {
		m.reporter(ctx, obs)
	}
	return &obs
}

func (m *Manager) marker(name string) string {
	return filepath.Join(m.stateDir, name)
}

func (m *Manager) Restore(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec) (bool, *client.AgentStateObserved) {
	if spec == nil {
		return true, nil
	}
	if _, err := os.Stat(m.marker(restoredMarker)); err == nil {
		return true, &client.AgentStateObserved{ObservedState: client.StateRestored}
	}
	if spec.Pending {
		return false, m.report(ctx, client.AgentStateObserved{ObservedState: client.StateWaiting})
	}
	if spec.Restore == nil {
		if err := os.WriteFile(m.marker(restoredMarker), []byte("none"), 0o644); err != nil {
			return false, m.failed(ctx, client.StateRestoreFailed, err)
		}
		return true, &client.AgentStateObserved{ObservedState: client.StateRestored}
	}

	root := content.WorkspaceRoot(container)
	if root == "" {
		return false, m.failed(ctx, client.StateRestoreFailed, errors.New("no /workspace volume to restore state into"))
	}
	m.report(ctx, client.AgentStateObserved{ObservedState: client.StateRestoring})
	if err := m.restore(ctx, spec.Restore, root); err != nil {
		return false, m.failed(ctx, client.StateRestoreFailed, err)
	}
	if err := os.WriteFile(m.marker(restoredMarker), []byte(spec.Restore.SHA256), 0o644); err != nil {
		return false, m.failed(ctx, client.StateRestoreFailed, err)
	}
	m.log.Info("workspace state restored", "bytes", spec.Restore.SizeBytes)
	return true, m.report(ctx, client.AgentStateObserved{ObservedState: client.StateRestored})
}

func (m *Manager) restore(ctx context.Context, spec *client.StateRestore, root string) error {
	if err := os.MkdirAll(root, 0o777); err != nil {
		return err
	}
	archive := m.marker("state-in.tar.zst")
	defer os.Remove(archive)
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = m.download(ctx, spec, archive); err == nil {
			break
		}
		m.log.Warn("state download failed", "attempt", attempt, "err", err)
		if ctx.Err() != nil || attempt == attempts {
			return err
		}
		time.Sleep(m.retryDelay)
	}
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	return Unpack(f, root, maxUnpacked)
}

func (m *Manager) download(ctx context.Context, spec *client.StateRestore, dest string) (err error) {
	defer func() {
		if err != nil {
			os.Remove(dest)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, spec.URL, nil)
	if err != nil {
		return err
	}
	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("state download: http %d", resp.StatusCode)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, spec.SizeBytes+1))
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if n != spec.SizeBytes {
		return fmt.Errorf("state size %d, expected %d", n, spec.SizeBytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != spec.SHA256 {
		return fmt.Errorf("state sha256 mismatch: %s", got)
	}
	return nil
}

func (m *Manager) Freeze(ctx context.Context, spec *client.AgentStateSpec, containerName string) {
	if spec == nil || spec.Save == nil || len(spec.FreezeCommand) == 0 {
		return
	}
	if _, err := os.Stat(m.marker(savedMarker)); err == nil {
		return
	}
	args := append([]string{"exec", containerName}, spec.FreezeCommand...)
	if _, err := m.exec.Run(ctx, freezeTimeout, "docker", args...); err != nil {
		m.log.Warn("state freeze command failed", "err", err)
	}
}

func (m *Manager) Save(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec) *client.AgentStateObserved {
	if spec == nil || spec.Save == nil {
		return nil
	}
	if raw, err := os.ReadFile(m.marker(savedMarker)); err == nil {
		sum, sizeText, _ := strings.Cut(strings.TrimSpace(string(raw)), " ")
		size, _ := strconv.ParseInt(sizeText, 10, 64)
		return &client.AgentStateObserved{ObservedState: client.StateSaved, SHA256: &sum, SizeBytes: &size}
	}
	root := content.WorkspaceRoot(container)
	if root == "" {
		return m.failed(ctx, client.StateSaveFailed, errors.New("no /workspace volume to save state from"))
	}
	m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaving})

	archive := m.marker("state-out.tar.zst")
	defer os.Remove(archive)
	sum, size, err := packToFile(root, spec, archive)
	if err == nil && size > maxUpload {
		err = fmt.Errorf("state is %d bytes, limit %d", size, maxUpload)
	}
	for attempt := 1; err == nil && attempt <= attempts; attempt++ {
		if err = m.upload(ctx, spec.Save.UploadURL, archive, size); err == nil {
			break
		}
		m.log.Warn("state upload failed", "attempt", attempt, "err", err)
		if ctx.Err() != nil || attempt == attempts {
			break
		}
		err = nil
		time.Sleep(m.retryDelay)
	}
	if err != nil {
		return m.failed(ctx, client.StateSaveFailed, err)
	}
	if err := os.WriteFile(m.marker(savedMarker), []byte(fmt.Sprintf("%s %d", sum, size)), 0o644); err != nil {
		return m.failed(ctx, client.StateSaveFailed, err)
	}
	m.log.Info("workspace state saved", "bytes", size)
	return m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaved, SHA256: &sum, SizeBytes: &size})
}

func packToFile(root string, spec *client.AgentStateSpec, dest string) (string, int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(f, h)}
	if err := Pack(root, spec.Include, spec.Exclude, counter); err != nil {
		f.Close()
		return "", 0, err
	}
	if err := f.Close(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), counter.n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func (m *Manager) upload(ctx context.Context, url, file string, size int64) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, f)
	if err != nil {
		return err
	}
	req.ContentLength = size
	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("state upload: http %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (m *Manager) failed(ctx context.Context, state string, err error) *client.AgentStateObserved {
	msg := err.Error()
	if len(msg) > 1024 {
		msg = msg[:1024]
	}
	m.log.Error("workspace state failed", "state", state, "err", err)
	return m.report(ctx, client.AgentStateObserved{ObservedState: state, LastError: &msg})
}

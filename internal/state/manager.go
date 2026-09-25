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
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/container"
	"github.com/bogdanaks/yougpu-agent/internal/content"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

const (
	restoredMarker  = "state_restored"
	savedMarker     = "state_saved"
	skippedMarker   = "state_save_skipped"
	failedMarker    = "state_save_failed"
	saveStartMarker = "state_save_started"
	restoreDir      = ".yougpu-restore"
	maxUnpacked     = 20 << 30
	maxEntries      = 500_000
	maxUpload       = 5 << 30
	maxError        = 1024
	attempts        = 3
	freezeTimeout   = 2 * time.Minute
	restoreTimeout  = 30 * time.Minute
	saveTimeout     = 30 * time.Minute
	saveWindow      = 10 * time.Minute
	idleTimeout     = time.Minute
	restoringEvery  = 30 * time.Second
	retryDelay      = 5 * time.Second
)

type Manager struct {
	stateDir       string
	exec           system.Executor
	http           *http.Client
	log            *slog.Logger
	reporter       func(context.Context, client.AgentStateObserved)
	notify         func()
	retryDelay     time.Duration
	idle           time.Duration
	restoreTimeout time.Duration
	saveTimeout    time.Duration
	saveWindow     time.Duration
	restoringEvery time.Duration
	limits         Limits
	maxUpload      int64

	mu  sync.Mutex
	job *restoreJob
}

type restoreJob struct {
	sha     string
	size    int64
	cancel  context.CancelFunc
	stopped atomic.Bool
	done    chan struct{}
	result  *client.AgentStateObserved
}

type finalError struct{ err error }

func (e finalError) Error() string { return e.err.Error() }
func (e finalError) Unwrap() error { return e.err }

func New(stateDir string, exec system.Executor, log *slog.Logger) *Manager {
	return &Manager{
		stateDir:       stateDir,
		exec:           exec,
		http:           content.NewHTTPClient(),
		log:            log,
		retryDelay:     retryDelay,
		idle:           idleTimeout,
		restoreTimeout: restoreTimeout,
		saveTimeout:    saveTimeout,
		saveWindow:     saveWindow,
		restoringEvery: restoringEvery,
		limits:         Limits{Bytes: maxUnpacked, Entries: maxEntries},
		maxUpload:      maxUpload,
	}
}

func (m *Manager) SetReporter(f func(context.Context, client.AgentStateObserved)) {
	m.reporter = f
}

func (m *Manager) SetNotify(f func()) {
	m.notify = f
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

func (m *Manager) has(name string) bool {
	_, err := os.Stat(m.marker(name))
	return err == nil
}

func (m *Manager) Restore(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec) (bool, *client.AgentStateObserved) {
	if spec == nil {
		return true, nil
	}
	if m.has(restoredMarker) {
		return true, &client.AgentStateObserved{ObservedState: client.StateRestored}
	}
	if spec.Pending {
		return false, &client.AgentStateObserved{ObservedState: client.StateWaiting}
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
	return false, m.startRestore(ctx, spec, root)
}

func (m *Manager) startRestore(ctx context.Context, spec *client.AgentStateSpec, root string) *client.AgentStateObserved {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j := m.job; j != nil {
		same := j.sha == spec.Restore.SHA256 && j.size == spec.Restore.SizeBytes
		select {
		case <-j.done:
			if same && j.result != nil {
				return j.result
			}
		default:
			if same {
				return &client.AgentStateObserved{ObservedState: client.StateRestoring}
			}
			j.stopped.Store(true)
			j.cancel()
			<-j.done
		}
	}
	jobCtx, cancel := context.WithTimeout(ctx, m.restoreTimeout)
	j := &restoreJob{sha: spec.Restore.SHA256, size: spec.Restore.SizeBytes, cancel: cancel, done: make(chan struct{})}
	m.job = j
	go m.runRestore(ctx, jobCtx, j, spec, root)
	return &client.AgentStateObserved{ObservedState: client.StateRestoring}
}

func (m *Manager) runRestore(parent, ctx context.Context, j *restoreJob, spec *client.AgentStateSpec, root string) {
	defer close(j.done)
	defer j.cancel()
	stopReports := m.keepReporting(ctx)
	err := m.restore(ctx, spec, root)
	stopReports()
	if j.stopped.Load() || parent.Err() != nil {
		return
	}
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("state restore did not finish in %s", m.restoreTimeout)
	}
	if err == nil {
		err = os.WriteFile(m.marker(restoredMarker), []byte(spec.Restore.SHA256), 0o644)
	}
	if err != nil {
		j.result = m.failed(parent, client.StateRestoreFailed, err)
	} else {
		m.log.Info("workspace state restored", "bytes", spec.Restore.SizeBytes)
		j.result = m.report(parent, client.AgentStateObserved{ObservedState: client.StateRestored})
	}
	if m.notify != nil {
		m.notify()
	}
}

func (m *Manager) keepReporting(ctx context.Context) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(m.restoringEvery)
		defer ticker.Stop()
		for {
			m.report(ctx, client.AgentStateObserved{ObservedState: client.StateRestoring})
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func (m *Manager) stopRestore() {
	m.mu.Lock()
	j := m.job
	m.mu.Unlock()
	if j == nil {
		return
	}
	j.stopped.Store(true)
	j.cancel()
	<-j.done
}

func (m *Manager) restore(ctx context.Context, spec *client.AgentStateSpec, root string) error {
	if err := os.MkdirAll(root, 0o777); err != nil {
		return err
	}
	archive := m.marker("state-in.tar.zst")
	defer os.Remove(archive)
	for attempt := 1; ; attempt++ {
		err := m.download(ctx, spec.Restore, archive)
		if err == nil {
			break
		}
		m.log.Warn("state download failed", "attempt", attempt, "err", err)
		if ctx.Err() != nil || attempt == attempts {
			return err
		}
		if err := sleep(ctx, m.retryDelay); err != nil {
			return err
		}
	}
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	tmp := filepath.Join(root, restoreDir)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	if err := os.Mkdir(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := Unpack(content.CtxReader(ctx, f), tmp, spec.Include, m.limits); err != nil {
		return err
	}
	return moveInto(tmp, root)
}

func (m *Manager) download(ctx context.Context, spec *client.StateRestore, dest string) (err error) {
	defer func() {
		if err != nil {
			os.Remove(dest)
		}
	}()
	resp, err := content.Get(ctx, m.http, spec.URL, "", m.idle)
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

func (m *Manager) packable() bool {
	return m.has(restoredMarker) && m.has(container.StartedMarker)
}

func (m *Manager) Freeze(ctx context.Context, spec *client.AgentStateSpec, containerName string) {
	if spec == nil || spec.Save == nil {
		return
	}
	m.stopRestore()
	if m.Outcome() != nil || !m.packable() {
		return
	}
	m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaving})
	if len(spec.FreezeCommand) == 0 {
		return
	}
	args := append([]string{"exec", containerName}, spec.FreezeCommand...)
	if _, err := m.exec.Run(ctx, freezeTimeout, "docker", args...); err != nil {
		m.log.Warn("state freeze command failed", "err", err)
	}
}

func (m *Manager) Outcome() *client.AgentStateObserved {
	if raw, err := os.ReadFile(m.marker(savedMarker)); err == nil {
		sum, sizeText, _ := strings.Cut(strings.TrimSpace(string(raw)), " ")
		size, _ := strconv.ParseInt(sizeText, 10, 64)
		return &client.AgentStateObserved{ObservedState: client.StateSaved, SHA256: &sum, SizeBytes: &size}
	}
	if m.has(skippedMarker) {
		return &client.AgentStateObserved{ObservedState: client.StateSaveSkipped}
	}
	if raw, err := os.ReadFile(m.marker(failedMarker)); err == nil {
		msg := string(raw)
		return &client.AgentStateObserved{ObservedState: client.StateSaveFailed, LastError: &msg}
	}
	return nil
}

func (m *Manager) Save(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec, stopErr error) *client.AgentStateObserved {
	if spec == nil || spec.Save == nil {
		return nil
	}
	m.stopRestore()
	if obs := m.Outcome(); obs != nil {
		return obs
	}
	if !m.packable() {
		if err := os.WriteFile(m.marker(skippedMarker), nil, 0o644); err != nil {
			m.log.Warn("could not persist skipped save", "err", err)
		}
		m.log.Info("workspace state not saved: it was never restored or ComfyUI never ran here")
		return m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaveSkipped})
	}
	started := m.saveStarted()
	if stopErr != nil {
		return m.saveFailed(ctx, started, stopErr)
	}
	root := content.WorkspaceRoot(container)
	if root == "" {
		return m.saveFailed(ctx, started, finalError{errors.New("no /workspace volume to save state from")})
	}
	m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaving})
	sum, size, err := m.save(ctx, spec, root)
	if err != nil {
		return m.saveFailed(ctx, started, err)
	}
	if err := os.WriteFile(m.marker(savedMarker), []byte(fmt.Sprintf("%s %d", sum, size)), 0o644); err != nil {
		m.log.Warn("could not persist saved state", "err", err)
	}
	m.log.Info("workspace state saved", "bytes", size)
	return m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaved, SHA256: &sum, SizeBytes: &size})
}

func (m *Manager) saveStarted() time.Time {
	if raw, err := os.ReadFile(m.marker(saveStartMarker)); err == nil {
		if ms, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil {
			return time.UnixMilli(ms)
		}
	}
	now := time.Now()
	if err := os.WriteFile(m.marker(saveStartMarker), []byte(strconv.FormatInt(now.UnixMilli(), 10)), 0o644); err != nil {
		m.log.Warn("could not persist save start", "err", err)
	}
	return now
}

func (m *Manager) saveFailed(ctx context.Context, started time.Time, err error) *client.AgentStateObserved {
	var final finalError
	if errors.As(err, &final) || errors.Is(err, errTooLarge) || time.Since(started) >= m.saveWindow {
		obs := m.failed(ctx, client.StateSaveFailed, err)
		if werr := os.WriteFile(m.marker(failedMarker), []byte(*obs.LastError), 0o644); werr != nil {
			m.log.Warn("could not persist failed save", "err", werr)
		}
		return obs
	}
	msg := content.Clip(content.Redact(err).Error(), maxError)
	m.log.Warn("workspace state save attempt failed, retrying on next tick", "err", msg, "until", started.Add(m.saveWindow))
	return m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaving, LastError: &msg})
}

func (m *Manager) save(ctx context.Context, spec *client.AgentStateSpec, root string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, m.saveTimeout)
	defer cancel()
	archive := m.marker("state-out.tar.zst")
	defer os.Remove(archive)
	sum, size, err := m.packToFile(ctx, root, spec, archive)
	if err != nil {
		return "", 0, err
	}
	for attempt := 1; ; attempt++ {
		err = m.upload(ctx, spec.Save.UploadURL, archive, size)
		if err == nil {
			return sum, size, nil
		}
		m.log.Warn("state upload failed", "attempt", attempt, "err", err)
		if ctx.Err() != nil || attempt == attempts {
			return "", 0, err
		}
		if err := sleep(ctx, m.retryDelay); err != nil {
			return "", 0, err
		}
	}
}

func (m *Manager) packToFile(ctx context.Context, root string, spec *client.AgentStateSpec, dest string) (string, int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	out := &limitedWriter{ctx: ctx, w: io.MultiWriter(f, h), limit: m.maxUpload}
	if err := Pack(root, spec.Include, spec.Exclude, out, m.limits); err != nil {
		f.Close()
		return "", 0, err
	}
	if err := f.Close(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), out.n, nil
}

type limitedWriter struct {
	ctx   context.Context
	w     io.Writer
	n     int64
	limit int64
}

func (c *limitedWriter) Write(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	if c.n+int64(len(p)) > c.limit {
		return 0, fmt.Errorf("%w: archive is over %d bytes", errTooLarge, c.limit)
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func (m *Manager) upload(ctx context.Context, rawURL, file string, size int64) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, f)
	if err != nil {
		return content.Redact(err)
	}
	req.ContentLength = size
	resp, err := m.http.Do(req)
	if err != nil {
		return content.Redact(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("state upload: http %d%s", resp.StatusCode, errorCode(body))
	}
	return nil
}

func errorCode(body []byte) string {
	s := string(body)
	start := strings.Index(s, "<Code>")
	end := strings.Index(s, "</Code>")
	if start < 0 || end < start+len("<Code>") {
		return ""
	}
	return " " + content.Clip(s[start+len("<Code>"):end], 64)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (m *Manager) failed(ctx context.Context, state string, err error) *client.AgentStateObserved {
	err = content.Redact(err)
	msg := content.Clip(err.Error(), maxError)
	m.log.Error("workspace state failed", "state", state, "err", msg)
	return m.report(ctx, client.AgentStateObserved{ObservedState: state, LastError: &msg})
}

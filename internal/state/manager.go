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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/container"
	"github.com/bogdanaks/yougpu-agent/internal/fetch"
)

const (
	restoredMarker    = "state_restored"
	savedMarker       = "state_saved"
	skippedMarker     = "state_save_skipped"
	failedMarker      = "state_save_failed"
	saveStartMarker   = "state_save_started"
	restoreDir        = ".yougpu-restore"
	maxUnpacked       = 20 << 30
	maxEntries        = 500_000
	packedMarker      = "state_packed"
	inArchive         = "state-in.tar.zst"
	outArchive        = "state-out.tar.zst"
	overlayArchive    = "state-overlay.tar.zst"
	checkpointArchive = "state-checkpoint.tar.zst"
	venvDir           = ".venv"
	maxUpload         = 5_000_000_000
	maxError          = 1024
	restoreTimeout    = 30 * time.Minute
	saveTimeout       = 30 * time.Minute
	checkpointTimeout = 15 * time.Minute
	saveWindow        = 30 * time.Minute
	maxRetryDelay     = 2 * time.Minute
	idleTimeout       = time.Minute
	restoringEvery    = 30 * time.Second
	retryDelay        = 5 * time.Second
)

type Backend interface {
	CheckpointURL(ctx context.Context) (*client.CheckpointUpload, error)
	CommitCheckpoint(ctx context.Context, commit client.CheckpointCommit) error
}

type Manager struct {
	stateDir       string
	backend        Backend
	now            func() time.Time
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
	uploadFails    int
	uploadErr      error
	nextUpload     time.Time

	checkpointTimeout time.Duration

	mu            sync.Mutex
	job           *restoreJob
	checkpoint    *checkpointJob
	checkpointAt  time.Time
	checkpointSum string
	sealed        bool
}

type restoreJob struct {
	key     string
	cancel  context.CancelFunc
	stopped atomic.Bool
	done    chan struct{}
	result  *client.AgentStateObserved
}

type finalError struct{ err error }

func (e finalError) Error() string { return e.err.Error() }
func (e finalError) Unwrap() error { return e.err }

func New(stateDir string, backend Backend, log *slog.Logger) *Manager {
	return &Manager{
		stateDir:          stateDir,
		backend:           backend,
		now:               time.Now,
		http:              fetch.NewHTTPClient(),
		log:               log,
		retryDelay:        retryDelay,
		idle:              idleTimeout,
		restoreTimeout:    restoreTimeout,
		saveTimeout:       saveTimeout,
		checkpointTimeout: checkpointTimeout,
		saveWindow:        saveWindow,
		restoringEvery:    restoringEvery,
		limits:            Limits{Bytes: maxUnpacked, Entries: maxEntries},
		maxUpload:         maxUpload,
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
	if spec.Restore == nil && spec.Overlay == nil {
		if err := os.WriteFile(m.marker(restoredMarker), []byte("none"), 0o644); err != nil {
			return false, m.failed(ctx, client.StateRestoreFailed, err)
		}
		return true, &client.AgentStateObserved{ObservedState: client.StateRestored}
	}
	root := fetch.WorkspaceRoot(container)
	if root == "" {
		return false, m.failed(ctx, client.StateRestoreFailed, errors.New("no /workspace volume to restore state into"))
	}
	return false, m.startRestore(ctx, spec, root)
}

func (m *Manager) startRestore(ctx context.Context, spec *client.AgentStateSpec, root string) *client.AgentStateObserved {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := restoreKey(spec)
	if j := m.job; j != nil {
		same := j.key == key
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
	j := &restoreJob{key: key, cancel: cancel, done: make(chan struct{})}
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
	if err == nil {
		err = os.WriteFile(m.marker(restoredMarker), []byte(j.key), 0o644)
	}
	if err != nil {
		j.result = m.failed(parent, client.StateRestoreFailed, err)
	} else {
		m.log.Info("workspace state restored", "archive", j.key)
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

func restoreKey(spec *client.AgentStateSpec) string {
	key := archiveKey(spec.Restore)
	if spec.Overlay != nil {
		key += "+" + archiveKey(&spec.Overlay.StateRestore)
	}
	return key
}

func archiveKey(a *client.StateRestore) string {
	if a == nil {
		return "none"
	}
	return fmt.Sprintf("%s:%d", a.SHA256, a.SizeBytes)
}

func (m *Manager) restore(ctx context.Context, spec *client.AgentStateSpec, root string) error {
	if err := os.MkdirAll(root, 0o777); err != nil {
		return err
	}
	full, overlay := m.marker(inArchive), m.marker(overlayArchive)
	defer os.Remove(full)
	defer os.Remove(overlay)
	if spec.Restore != nil {
		if err := m.fetchVerified(ctx, spec.Restore, full); err != nil {
			return err
		}
	}
	if spec.Overlay != nil {
		if err := m.fetchVerified(ctx, &spec.Overlay.StateRestore, overlay); err != nil {
			return fmt.Errorf("overlay: %w", err)
		}
	}
	tmp := filepath.Join(root, restoreDir)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	if err := os.Mkdir(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if spec.Restore != nil {
		if err := m.unpackFile(ctx, full, tmp, spec.Include); err != nil {
			return err
		}
	}
	if spec.Overlay != nil {
		for _, dir := range spec.Overlay.Include {
			rel, err := safeRel(dir)
			if err != nil {
				return fmt.Errorf("overlay: %w", err)
			}
			if err := os.RemoveAll(filepath.Join(tmp, filepath.FromSlash(rel))); err != nil {
				return err
			}
		}
		if err := m.unpackFile(ctx, overlay, tmp, spec.Overlay.Include); err != nil {
			return fmt.Errorf("overlay: %w", err)
		}
	}
	return moveInto(tmp, root)
}

func (m *Manager) fetchVerified(ctx context.Context, spec *client.StateRestore, dest string) error {
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := m.fetchArchive(ctx, spec, dest); err != nil {
		return err
	}
	return verify(ctx, dest, spec.SHA256)
}

func (m *Manager) unpackFile(ctx context.Context, archive, dest string, include []string) error {
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := Unpack(fetch.CtxReader(ctx, f), dest, include, m.limits); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("state unpack: did not finish in %s", m.restoreTimeout)
		}
		return fmt.Errorf("state unpack: %w", err)
	}
	return nil
}

func (m *Manager) fetchArchive(ctx context.Context, spec *client.StateRestore, dest string) error {
	delay := m.retryDelay
	var cause error
	for attempt := 1; ; attempt++ {
		err := m.download(ctx, spec, dest)
		if err == nil {
			return nil
		}
		if ctx.Err() == nil || cause == nil {
			cause = err
		}
		var final finalError
		if errors.As(err, &final) {
			return fmt.Errorf("state download: %w", err)
		}
		if ctx.Err() == nil {
			m.log.Warn("state download failed, retrying", "attempt", attempt, "pause", delay.String(), "err", err)
			if sleep(ctx, delay) == nil {
				delay = min(delay*2, maxRetryDelay)
				continue
			}
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("state download: did not finish in %s: %w", m.restoreTimeout, cause)
		}
		return fmt.Errorf("state download: %w", cause)
	}
}

func (m *Manager) download(ctx context.Context, spec *client.StateRestore, dest string) error {
	f, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	have, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if have > spec.SizeBytes {
		if have, err = restart(f); err != nil {
			return err
		}
	}
	if have == spec.SizeBytes {
		return nil
	}
	rng := ""
	if have > 0 {
		rng = fmt.Sprintf("bytes=%d-", have)
	}
	resp, err := fetch.Get(ctx, m.http, spec.URL, rng, m.idle)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch code := resp.StatusCode; {
	case code == http.StatusPartialContent && have > 0 && rangeStart(resp.Header.Get("Content-Range")) == have:
	case code == http.StatusOK:
		if have, err = restart(f); err != nil {
			return err
		}
	case code == http.StatusPartialContent || code == http.StatusRequestedRangeNotSatisfiable:
		_, _ = restart(f)
		return fmt.Errorf("http %d for bytes from %d", code, have)
	case code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests:
		return finalError{fmt.Errorf("http %d", code)}
	default:
		return fmt.Errorf("http %d", code)
	}
	n, err := io.Copy(f, io.LimitReader(resp.Body, spec.SizeBytes-have+1))
	have += n
	if err != nil {
		return err
	}
	if have > spec.SizeBytes {
		_, _ = restart(f)
		return finalError{fmt.Errorf("archive is larger than %d bytes", spec.SizeBytes)}
	}
	if have < spec.SizeBytes {
		return fmt.Errorf("got %d of %d bytes", have, spec.SizeBytes)
	}
	return nil
}

func restart(f *os.File) (int64, error) {
	if err := f.Truncate(0); err != nil {
		return 0, err
	}
	return f.Seek(0, io.SeekStart)
}

func rangeStart(contentRange string) int64 {
	spec, ok := strings.CutPrefix(contentRange, "bytes ")
	if !ok {
		return -1
	}
	from, _, _ := strings.Cut(spec, "-")
	n, err := strconv.ParseInt(from, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

func verify(ctx context.Context, path, want string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, fetch.CtxReader(ctx, f)); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		return fmt.Errorf("state sha256 mismatch: %s", got)
	}
	return nil
}

func (m *Manager) packable() bool {
	return m.has(restoredMarker) && m.has(container.StartedMarker)
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
	m.stopCheckpoints()
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
	root := fetch.WorkspaceRoot(container)
	if root == "" {
		return m.saveFailed(ctx, started, finalError{errors.New("no /workspace volume to save state from")})
	}
	if m.uploadErr != nil && time.Now().Before(m.nextUpload) {
		return m.saveFailed(ctx, started, m.uploadErr)
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
		m.dropArchive()
		obs := m.failed(ctx, client.StateSaveFailed, err)
		if werr := os.WriteFile(m.marker(failedMarker), []byte(*obs.LastError), 0o644); werr != nil {
			m.log.Warn("could not persist failed save", "err", werr)
		}
		return obs
	}
	msg := fetch.Clip(fetch.Redact(err).Error(), maxError)
	m.log.Warn("workspace state save attempt failed, retrying on next tick", "err", msg, "until", started.Add(m.saveWindow))
	return m.report(ctx, client.AgentStateObserved{ObservedState: client.StateSaving, LastError: &msg})
}

func (m *Manager) save(ctx context.Context, spec *client.AgentStateSpec, root string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, m.saveTimeout)
	defer cancel()
	archive := m.marker(outArchive)
	sum, size, err := m.packed(archive)
	if err != nil {
		if sum, size, err = m.pack(ctx, spec, root, archive); err != nil {
			return "", 0, err
		}
	}
	if err := m.upload(ctx, spec.Save.UploadURL, archive, size); err != nil {
		m.retryLater(err)
		return "", 0, err
	}
	m.dropArchive()
	return sum, size, nil
}

func (m *Manager) retryLater(err error) {
	delay := m.retryDelay
	for i := 0; i < m.uploadFails && delay < maxRetryDelay; i++ {
		delay *= 2
	}
	delay = min(delay, maxRetryDelay)
	m.uploadFails++
	m.uploadErr = err
	m.nextUpload = time.Now().Add(delay)
	m.log.Warn("state upload failed, will retry", "attempt", m.uploadFails, "pause", delay.String(), "err", err)
	if m.notify != nil {
		time.AfterFunc(delay, m.notify)
	}
}

func (m *Manager) packed(archive string) (string, int64, error) {
	raw, err := os.ReadFile(m.marker(packedMarker))
	if err != nil {
		return "", 0, err
	}
	sum, sizeText, _ := strings.Cut(strings.TrimSpace(string(raw)), " ")
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil {
		return "", 0, err
	}
	info, err := os.Stat(archive)
	if err != nil {
		return "", 0, err
	}
	if info.Size() != size {
		return "", 0, fmt.Errorf("packed archive is %d bytes, expected %d", info.Size(), size)
	}
	return sum, size, nil
}

func (m *Manager) pack(ctx context.Context, spec *client.AgentStateSpec, root, archive string) (string, int64, error) {
	sum, size, err := m.packToFile(ctx, root, spec.Include, spec.Exclude, archive, m.maxUpload)
	if errors.Is(err, errTooLarge) {
		m.log.Warn("workspace state is too large, saving it without .venv", "err", err)
		sum, size, err = m.packToFile(ctx, root, spec.Include, append(slices.Clone(spec.Exclude), venvDir), archive, m.maxUpload)
	}
	if err != nil {
		os.Remove(archive)
		return "", 0, err
	}
	if err := os.WriteFile(m.marker(packedMarker), []byte(fmt.Sprintf("%s %d", sum, size)), 0o644); err != nil {
		m.log.Warn("could not persist packed archive", "err", err)
	}
	return sum, size, nil
}

func (m *Manager) dropArchive() {
	for _, name := range []string{outArchive, packedMarker} {
		if err := os.Remove(m.marker(name)); err != nil && !os.IsNotExist(err) {
			m.log.Warn("could not remove packed archive", "file", name, "err", err)
		}
	}
}

func (m *Manager) packToFile(ctx context.Context, root string, include, exclude []string, dest string, limit int64) (string, int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	out := &limitedWriter{ctx: ctx, w: io.MultiWriter(f, h), limit: limit}
	if err := Pack(root, include, exclude, out, m.limits); err != nil {
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
		return fetch.Redact(err)
	}
	req.ContentLength = size
	resp, err := m.http.Do(req)
	if err != nil {
		return fetch.Redact(err)
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
	return " " + fetch.Clip(s[start+len("<Code>"):end], 64)
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
	err = fetch.Redact(err)
	msg := fetch.Clip(err.Error(), maxError)
	m.log.Error("workspace state failed", "state", state, "err", msg)
	return m.report(ctx, client.AgentStateObserved{ObservedState: state, LastError: &msg})
}

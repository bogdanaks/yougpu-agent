package content

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
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const (
	WorkspaceContainerPath = "/workspace"
	dirPerm                = 0o777
	reportInterval         = 2 * time.Second
	rangeParts             = 8
	rangeMinSize           = 64 << 20
	rangeAttempts          = 3
	idleTimeout            = time.Minute
)

type Manager struct {
	httpClient *http.Client
	logger     *slog.Logger
	reporter   func(context.Context, client.AgentContentObserved)
	rangeMin   int64
	idle       time.Duration
}

func New(logger *slog.Logger) *Manager {
	return &Manager{
		// Без общего timeout: модели весят десятки ГБ, отмена идёт по ctx.
		httpClient: &http.Client{},
		logger:     logger,
		rangeMin:   rangeMinSize,
		idle:       idleTimeout,
	}
}

func (m *Manager) SetRangeMinForTest(n int64) {
	m.rangeMin = n
}

func (m *Manager) SetIdleTimeoutForTest(d time.Duration) {
	m.idle = d
}

func (m *Manager) SetReporter(fn func(context.Context, client.AgentContentObserved)) {
	m.reporter = fn
}

type task struct {
	target string
	url    string
	inline string
	sha256 string
	label  string
	repo   *client.ContentRepo
}

// WorkspaceRoot ищет host-путь тома, смонтированного в контейнер как /workspace.
func WorkspaceRoot(spec *client.AgentContainerSpec) string {
	if spec == nil {
		return ""
	}
	for _, v := range spec.Volumes {
		if v.Container == WorkspaceContainerPath && v.Host != "" {
			return v.Host
		}
	}
	return ""
}

// Reconcile идемпотентно докачивает app-контент в /workspace. Уже лежащие файлы
// (совпавшие по sha256/размеру) пропускаются — повторные reconcile-тики не качают заново.
func (m *Manager) Reconcile(ctx context.Context, spec *client.AgentContentSpec, container *client.AgentContainerSpec) client.AgentContentObserved {
	if spec == nil || (len(spec.WorkspaceFiles) == 0 && len(spec.Models) == 0 && len(spec.Dirs) == 0 && len(spec.Repos) == 0) {
		return ready(nil)
	}
	root := WorkspaceRoot(container)
	if root == "" {
		return errObs("no /workspace volume to place content")
	}

	for _, d := range spec.Dirs {
		if err := os.MkdirAll(filepath.Join(root, filepath.Clean("/"+d)), dirPerm); err != nil {
			return errObs(fmt.Sprintf("mkdir %s: %s", d, truncate(err.Error(), 200)))
		}
	}

	pending, err := m.plan(spec, root)
	if err != nil {
		return errObs(err.Error())
	}
	if len(pending) == 0 {
		return ready(nil)
	}

	total := len(pending)
	prep := "подготовка"
	m.report(ctx, client.ContentDownloading, ptr(0), &prep)

	var failed []string
	for i, t := range pending {
		detail := t.label
		m.report(ctx, client.ContentDownloading, ptr(clamp(i*100/total)), &detail)

		idx := i
		// fraction — доля текущего файла (по Content-Length, если сервер его отдал).
		onProgress := func(fraction float64) {
			pct := int((float64(idx) + fraction) / float64(total) * 100)
			m.report(ctx, client.ContentDownloading, ptr(clamp(pct)), &detail)
		}
		if err := m.fetch(ctx, t, onProgress); err != nil {
			if ctx.Err() != nil {
				return errObs("cancelled")
			}
			m.logger.Error("content fetch failed", "label", t.label, "err", err)
			failed = append(failed, fmt.Sprintf("%s: %s", t.label, truncate(err.Error(), 200)))
		}
	}
	if len(failed) > 0 {
		obs := errObs(strings.Join(failed, "; "))
		m.reportObs(ctx, obs)
		return obs
	}

	m.logger.Info("content ready", "items", total)
	obs := ready(nil)
	m.reportObs(ctx, obs)
	return obs
}

// plan фильтрует уже присутствующие файлы (дедуп по sha256 / размеру / существованию).
func (m *Manager) plan(spec *client.AgentContentSpec, root string) ([]task, error) {
	var tasks []task

	for _, f := range spec.WorkspaceFiles {
		name := f.Name
		if name == "" {
			name = fileNameFromURL(f.URL)
		}
		if name == "" {
			return nil, fmt.Errorf("workspace file without name/url")
		}
		target := filepath.Join(root, filepath.Clean("/"+f.Dest), name)
		if fileExists(target) {
			continue
		}
		tasks = append(tasks, task{target: target, url: f.URL, inline: f.Content, label: name})
	}

	for _, mdl := range spec.Models {
		name := mdl.Name
		if name == "" {
			name = fileNameFromURL(mdl.URL)
		}
		if name == "" {
			return nil, fmt.Errorf("model without name/url")
		}
		target := filepath.Join(root, "models", filepath.Clean("/"+mdl.Type), name)
		if m.modelPresent(target, mdl) {
			continue
		}
		tasks = append(tasks, task{target: target, url: mdl.URL, sha256: mdl.SHA256, label: name})
	}

	for _, r := range spec.Repos {
		dest := filepath.Join(root, filepath.Clean("/"+r.Dest))
		if dirExists(dest) {
			continue
		}
		rr := r
		tasks = append(tasks, task{target: dest, label: fileNameFromURL(r.URL), repo: &rr})
	}

	return tasks, nil
}

func (m *Manager) modelPresent(target string, mdl client.ContentModel) bool {
	info, err := os.Stat(target)
	if err != nil {
		return false
	}
	if info.Size() == 0 {
		return false
	}
	if mdl.SHA256 != "" {
		sum, err := hashFile(target)
		if err != nil {
			return false
		}
		return strings.EqualFold(sum, mdl.SHA256)
	}
	if mdl.SizeBytes > 0 {
		return info.Size() == mdl.SizeBytes
	}
	return true
}

func (m *Manager) fetch(ctx context.Context, t task, onProgress func(float64)) error {
	if t.repo != nil {
		return m.clone(ctx, t)
	}
	if err := os.MkdirAll(filepath.Dir(t.target), dirPerm); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := t.target + ".part"

	if t.inline != "" && t.url == "" {
		if err := os.WriteFile(tmp, []byte(t.inline), 0o644); err != nil {
			return fmt.Errorf("write inline: %w", err)
		}
		return os.Rename(tmp, t.target)
	}

	if err := m.download(ctx, t.url, tmp, onProgress); err != nil {
		os.Remove(tmp)
		return err
	}

	if t.sha256 != "" {
		sum, err := hashFile(tmp)
		if err != nil {
			os.Remove(tmp)
			return fmt.Errorf("hash: %w", err)
		}
		if !strings.EqualFold(sum, t.sha256) {
			os.Remove(tmp)
			return fmt.Errorf("sha256 mismatch (got %s)", sum[:12])
		}
	}
	return os.Rename(tmp, t.target)
}

func (m *Manager) download(ctx context.Context, url, tmp string, onProgress func(float64)) error {
	resp, err := m.get(ctx, url, "bytes=0-")
	if err != nil {
		return err
	}

	total := int64(0)
	if resp.StatusCode == http.StatusPartialContent {
		total = totalFromContentRange(resp.Header.Get("Content-Range"))
		if total >= m.rangeMin {
			resp.Body.Close()
			return m.downloadRanged(ctx, url, tmp, total, onProgress)
		}
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("http %d", resp.StatusCode)
	}

	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	pw := &progressWriter{total: resp.ContentLength, onProgress: onProgress, throttle: reportInterval}
	written, err := io.Copy(io.MultiWriter(out, pw), resp.Body)
	if err != nil {
		out.Close()
		return fmt.Errorf("download: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if total > 0 && written != total {
		return fmt.Errorf("download: got %d of %d bytes", written, total)
	}
	return nil
}

func totalFromContentRange(v string) int64 {
	i := strings.LastIndex(v, "/")
	if i < 0 {
		return 0
	}
	total, err := strconv.ParseInt(strings.TrimSpace(v[i+1:]), 10, 64)
	if err != nil {
		return 0
	}
	return total
}

func (m *Manager) downloadRanged(ctx context.Context, url, tmp string, size int64, onProgress func(float64)) error {
	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	if err := out.Truncate(size); err != nil {
		out.Close()
		return fmt.Errorf("truncate: %w", err)
	}

	partCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pw := &progressWriter{total: size, onProgress: onProgress, throttle: reportInterval}
	chunk := size / rangeParts
	errs := make([]error, rangeParts)
	var wg sync.WaitGroup
	for i := range rangeParts {
		start := int64(i) * chunk
		end := start + chunk - 1
		if i == rangeParts-1 {
			end = size - 1
		}
		wg.Add(1)
		go func(idx int, from, to int64) {
			defer wg.Done()
			if err := m.downloadPart(partCtx, url, out, from, to, pw); err != nil {
				errs[idx] = err
				cancel()
			}
		}(i, start, end)
	}
	wg.Wait()

	if err := out.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	for _, e := range errs {
		if e != nil {
			return fmt.Errorf("download: %w", e)
		}
	}
	return nil
}

func (m *Manager) downloadPart(ctx context.Context, url string, out *os.File, from, to int64, pw *progressWriter) error {
	var lastErr error
	for attempt := 0; attempt < rangeAttempts; attempt++ {
		n, err := m.downloadRange(ctx, url, out, from, to, pw)
		from += n
		if from > to {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			lastErr = err
			continue
		}
		lastErr = fmt.Errorf("short body: %d bytes missing", to-from+1)
	}
	return lastErr
}

func (m *Manager) downloadRange(ctx context.Context, url string, out *os.File, from, to int64, pw *progressWriter) (int64, error) {
	resp, err := m.get(ctx, url, fmt.Sprintf("bytes=%d-%d", from, to))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("http %d for range %d-%d", resp.StatusCode, from, to)
	}
	return io.Copy(io.MultiWriter(io.NewOffsetWriter(out, from), pw), resp.Body)
}

func (m *Manager) get(ctx context.Context, url, rng string) (*http.Response, error) {
	reqCtx, watch := newIdleWatch(ctx, m.idle)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		watch.stop()
		return nil, err
	}
	req.Header.Set("Range", rng)
	resp, err := m.httpClient.Do(req)
	if err != nil {
		watch.stop()
		return nil, watch.explain(err)
	}
	resp.Body = &idleBody{ReadCloser: resp.Body, watch: watch}
	return resp, nil
}

type idleWatch struct {
	idle   time.Duration
	cancel context.CancelFunc
	timer  *time.Timer
	fired  atomic.Bool
}

func newIdleWatch(ctx context.Context, idle time.Duration) (context.Context, *idleWatch) {
	reqCtx, cancel := context.WithCancel(ctx)
	w := &idleWatch{idle: idle, cancel: cancel}
	w.timer = time.AfterFunc(idle, func() {
		w.fired.Store(true)
		cancel()
	})
	return reqCtx, w
}

func (w *idleWatch) explain(err error) error {
	if err != nil && !errors.Is(err, io.EOF) && w.fired.Load() {
		return fmt.Errorf("no data for %s", w.idle)
	}
	return err
}

func (w *idleWatch) stop() {
	w.timer.Stop()
	w.cancel()
}

type idleBody struct {
	io.ReadCloser
	watch *idleWatch
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.watch.timer.Reset(b.watch.idle)
	}
	return n, b.watch.explain(err)
}

func (b *idleBody) Close() error {
	err := b.ReadCloser.Close()
	b.watch.stop()
	return err
}

func (m *Manager) clone(ctx context.Context, t task) error {
	if err := os.MkdirAll(filepath.Dir(t.target), dirPerm); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	args := []string{"clone", "--depth", "1"}
	if t.repo.Ref != "" {
		args = append(args, "--branch", t.repo.Ref)
	}
	args = append(args, t.repo.URL, t.target)
	if out, err := exec.CommandContext(ctx, "git", args...).CombinedOutput(); err != nil {
		os.RemoveAll(t.target)
		return fmt.Errorf("git clone: %w: %s", err, truncate(strings.TrimSpace(string(out)), 200))
	}
	return nil
}

func (m *Manager) report(ctx context.Context, state string, progress *int, detail *string) {
	m.reportObs(ctx, client.AgentContentObserved{ObservedState: state, Progress: progress, Detail: detail})
}

func (m *Manager) reportObs(ctx context.Context, obs client.AgentContentObserved) {
	if m.reporter == nil {
		return
	}
	m.reporter(ctx, obs)
}

type progressWriter struct {
	mu         sync.Mutex
	onProgress func(float64)
	total      int64
	read       int64
	last       time.Time
	throttle   time.Duration
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n := len(b)

	p.mu.Lock()
	p.read += int64(n)
	report := p.onProgress != nil && time.Since(p.last) >= p.throttle
	frac := 0.0
	if report {
		p.last = time.Now()
		if p.total > 0 {
			frac = float64(p.read) / float64(p.total)
		}
	}
	p.mu.Unlock()

	if report {
		p.onProgress(frac)
	}
	return n, nil
}

func ready(detail *string) client.AgentContentObserved {
	return client.AgentContentObserved{ObservedState: client.ContentReady, Progress: ptr(100), Detail: detail}
}

func errObs(msg string) client.AgentContentObserved {
	e := truncate(msg, 1024)
	return client.AgentContentObserved{ObservedState: client.ContentError, LastError: &e}
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

func dirExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

func hashFile(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fileNameFromURL(u string) string {
	if u == "" {
		return ""
	}
	if i := strings.IndexAny(u, "?#"); i >= 0 {
		u = u[:i]
	}
	return path.Base(strings.TrimRight(u, "/"))
}

func ptr(i int) *int { return &i }

func clamp(i int) int {
	if i < 0 {
		return 0
	}
	if i > 100 {
		return 100
	}
	return i
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

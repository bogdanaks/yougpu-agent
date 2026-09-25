package content

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
	"unicode/utf16"

	"golang.org/x/sys/unix"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const (
	WorkspaceContainerPath = "/workspace"
	dirPerm                = 0o777
	filePerm               = 0o644
	reportInterval         = 2 * time.Second
	rangeParts             = 8
	rangeMinSize           = 64 << 20
	rangeAttempts          = 3
	idleTimeout            = time.Minute
	headerTimeout          = 2 * time.Minute
	fetchAttempts          = 3
	retryBase              = time.Minute
	maxDetail              = 255
	maxError               = 1024
	maxItemError           = 200
	maxSegment             = 255
	hashBuffer             = 1 << 20
)

type Manager struct {
	httpClient *http.Client
	logger     *slog.Logger
	rangeMin   int64
	idle       time.Duration
	retryBase  time.Duration
	reporter   func(context.Context, client.AgentContentObserved)
	notify     func()

	mu       sync.Mutex
	key      string
	cancel   context.CancelFunc
	done     chan struct{}
	observed client.AgentContentObserved
	settled  bool
	verified map[string]stamp
	failures map[string]*failure
}

type stamp struct {
	size    int64
	modTime time.Time
	sha256  string
}

type failure struct {
	attempts  int
	next      time.Time
	permanent bool
	msg       string
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func permanent(format string, args ...any) error {
	return permanentError{fmt.Errorf(format, args...)}
}

func isPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p) || errors.Is(err, syscall.ENOSPC)
}

func New(logger *slog.Logger) *Manager {
	return &Manager{
		httpClient: NewHTTPClient(),
		logger:     logger,
		rangeMin:   rangeMinSize,
		idle:       idleTimeout,
		retryBase:  retryBase,
		verified:   map[string]stamp{},
		failures:   map[string]*failure{},
	}
}

func NewHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = headerTimeout
	return &http.Client{Transport: transport}
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

func (m *Manager) SetNotify(fn func()) {
	m.notify = fn
}

type task struct {
	target string
	dir    string
	base   string
	url    string
	inline string
	sha256 string
	size   int64
	label  string
	model  bool
	repo   *client.ContentRepo
}

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

func (m *Manager) Reconcile(ctx context.Context, spec *client.AgentContentSpec, container *client.AgentContainerSpec) {
	root := WorkspaceRoot(container)
	key := specKey(spec, root)

	m.mu.Lock()
	if key != m.key {
		cancel, done := m.cancel, m.done
		m.mu.Unlock()
		if cancel != nil {
			cancel()
			<-done
		}
		m.mu.Lock()
		m.key = key
		m.cancel, m.done = nil, nil
		m.failures = map[string]*failure{}
		m.settled = false
		prep := "подготовка"
		m.observed = client.AgentContentObserved{ObservedState: client.ContentDownloading, Progress: ptr(0), Detail: &prep}
	}
	if m.done != nil {
		select {
		case <-m.done:
		default:
			m.mu.Unlock()
			return
		}
	}
	passCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.cancel, m.done = cancel, done
	first := !m.settled
	m.mu.Unlock()

	go m.run(passCtx, cancel, done, spec, root, first)
}

func (m *Manager) Observe() (client.AgentContentObserved, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observed, m.settled
}

func (m *Manager) Stop() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.key = ""
	m.cancel, m.done = nil, nil
	m.settled = false
	m.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (m *Manager) run(ctx context.Context, cancel context.CancelFunc, done chan struct{}, spec *client.AgentContentSpec, root string, first bool) {
	defer close(done)
	defer cancel()
	obs, attempted := m.pass(ctx, spec, root)
	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	m.observed = obs
	m.settled = true
	m.mu.Unlock()
	if attempted {
		m.emit(ctx, obs)
	}
	if (attempted || first) && m.notify != nil {
		m.notify()
	}
}

func specKey(spec *client.AgentContentSpec, root string) string {
	raw, _ := json.Marshal(spec)
	h := fnv.New64a()
	_, _ = h.Write(raw)
	_, _ = h.Write([]byte(root))
	return strconv.FormatUint(h.Sum64(), 16)
}

func (m *Manager) pass(ctx context.Context, spec *client.AgentContentSpec, root string) (client.AgentContentObserved, bool) {
	if spec == nil || (len(spec.WorkspaceFiles) == 0 && len(spec.Models) == 0 && len(spec.Dirs) == 0 && len(spec.Repos) == 0) {
		return ready(), false
	}
	if root == "" {
		return errObs("no /workspace volume to place content"), false
	}
	ws, err := os.OpenRoot(root)
	if err != nil {
		if err := os.MkdirAll(root, dirPerm); err != nil {
			return errObs("workspace: " + err.Error()), false
		}
		if ws, err = os.OpenRoot(root); err != nil {
			return errObs("workspace: " + err.Error()), false
		}
	}
	defer ws.Close()

	var failed []string
	for _, d := range spec.Dirs {
		if err := mkdirAll(ws, cleanDir(d)); err != nil {
			failed = append(failed, fmt.Sprintf("mkdir %s: %s", d, Clip(err.Error(), maxItemError)))
		}
	}

	tasks, invalid := m.plan(ctx, ws, spec, root)
	if ctx.Err() != nil {
		return errObs("cancelled"), false
	}
	failed = append(failed, invalid...)

	now := time.Now()
	skip := make([]*failure, len(tasks))
	queued := 0
	m.mu.Lock()
	for i, t := range tasks {
		if f := m.failures[t.target]; f != nil && (f.permanent || now.Before(f.next)) {
			skip[i] = f
			continue
		}
		queued++
	}
	m.mu.Unlock()

	if queued == 0 {
		for _, f := range skip {
			if f != nil {
				failed = append(failed, f.msg)
			}
		}
		if len(failed) > 0 {
			return errObs(strings.Join(failed, "; ")), false
		}
		return ready(), false
	}

	m.mu.Lock()
	m.settled = false
	m.mu.Unlock()
	prep := "подготовка"
	m.report(ctx, client.ContentDownloading, ptr(0), &prep)

	idx := 0
	for i, t := range tasks {
		if skip[i] != nil {
			failed = append(failed, skip[i].msg)
			continue
		}
		detail := Clip(t.label, maxDetail)
		m.report(ctx, client.ContentDownloading, ptr(clamp(idx*100/queued)), &detail)
		current := idx
		onProgress := func(fraction float64) {
			pct := int((float64(current) + fraction) / float64(queued) * 100)
			m.report(ctx, client.ContentDownloading, ptr(clamp(pct)), &detail)
		}
		idx++
		err := m.fetch(ctx, ws, t, onProgress)
		if ctx.Err() != nil {
			return errObs("cancelled"), false
		}
		if err != nil {
			msg := fmt.Sprintf("%s: %s", t.label, Clip(err.Error(), maxItemError))
			m.logger.Error("content fetch failed", "label", t.label, "err", err)
			m.fail(t.target, msg, isPermanent(err))
			failed = append(failed, msg)
			continue
		}
		m.mu.Lock()
		delete(m.failures, t.target)
		m.mu.Unlock()
	}
	if len(failed) > 0 {
		return errObs(strings.Join(failed, "; ")), true
	}
	m.logger.Info("content ready", "items", queued)
	return ready(), true
}

func (m *Manager) fail(target, msg string, final bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f := m.failures[target]
	if f == nil {
		f = &failure{}
		m.failures[target] = f
	}
	f.attempts++
	f.msg = msg
	if final || f.attempts >= fetchAttempts {
		f.permanent = true
		return
	}
	f.next = time.Now().Add(m.retryBase << (f.attempts - 1))
}

func (m *Manager) plan(ctx context.Context, ws *os.Root, spec *client.AgentContentSpec, root string) ([]task, []string) {
	var tasks []task
	var invalid []string

	for _, f := range spec.WorkspaceFiles {
		name := f.Name
		if name == "" {
			name = fileNameFromURL(f.URL)
		}
		if name == "" {
			invalid = append(invalid, "workspace file without name/url")
			continue
		}
		rel := path.Join(cleanDir(f.Dest), name)
		if err := checkRel(rel); err != nil {
			invalid = append(invalid, fmt.Sprintf("%q: %s", Clip(name, maxItemError), err))
			continue
		}
		if regularFile(ws, rel) {
			continue
		}
		tasks = append(tasks, newTask(root, rel, f.URL, name, "", 0, f.Content))
	}

	type group struct {
		model    client.ContentModel
		label    string
		conflict bool
	}
	var order []string
	groups := map[string]*group{}
	for _, mdl := range spec.Models {
		name := mdl.Name
		if name == "" {
			name = fileNameFromURL(mdl.URL)
		}
		if name == "" {
			invalid = append(invalid, "model without name/url")
			continue
		}
		if err := checkModel(mdl.Type, name); err != nil {
			invalid = append(invalid, fmt.Sprintf("%q: %s", Clip(name, maxItemError), err))
			continue
		}
		rel := path.Join("models", mdl.Type, name)
		g, ok := groups[rel]
		if !ok {
			mdl.Name = name
			groups[rel] = &group{model: mdl, label: name}
			order = append(order, rel)
			continue
		}
		switch {
		case mdl.SHA256 != "" && g.model.SHA256 != "" && !strings.EqualFold(mdl.SHA256, g.model.SHA256):
			g.conflict = true
		case g.model.SHA256 == "" && mdl.SHA256 != "":
			g.model.SHA256 = mdl.SHA256
		}
		if g.model.SizeBytes == 0 {
			g.model.SizeBytes = mdl.SizeBytes
		}
	}
	for _, rel := range order {
		g := groups[rel]
		if g.conflict {
			invalid = append(invalid, fmt.Sprintf("%s: conflicting sha256 for %s", g.label, rel))
			continue
		}
		t := newTask(root, rel, g.model.URL, g.label, g.model.SHA256, g.model.SizeBytes, "")
		t.model = true
		if m.modelPresent(ctx, ws, rel, t) {
			continue
		}
		tasks = append(tasks, t)
	}

	for _, r := range spec.Repos {
		dest := filepath.Join(root, filepath.Clean("/"+r.Dest))
		if dirExists(dest) {
			continue
		}
		rr := r
		tasks = append(tasks, task{target: dest, label: fileNameFromURL(r.URL), repo: &rr})
	}
	return tasks, invalid
}

func newTask(root, rel, rawURL, label, sum string, size int64, inline string) task {
	dir, base := path.Split(rel)
	return task{
		target: filepath.Join(root, filepath.FromSlash(rel)),
		dir:    strings.TrimSuffix(dir, "/"),
		base:   base,
		url:    rawURL,
		inline: inline,
		sha256: sum,
		size:   size,
		label:  label,
	}
}

func cleanDir(p string) string {
	return strings.TrimPrefix(path.Clean("/"+p), "/")
}

func checkModel(folder, name string) error {
	if err := checkRel(folder); err != nil {
		return fmt.Errorf("model folder: %w", err)
	}
	if err := checkRel(name); err != nil {
		return fmt.Errorf("model name: %w", err)
	}
	return nil
}

func checkRel(p string) error {
	switch {
	case p == "":
		return errors.New("empty path")
	case strings.HasPrefix(p, "/"):
		return errors.New("absolute path")
	case strings.ContainsRune(p, '\\'):
		return errors.New("backslash in path")
	}
	for _, r := range p {
		if unicode.IsControl(r) {
			return errors.New("control character in path")
		}
	}
	for _, seg := range strings.Split(p, "/") {
		switch {
		case seg == "" || seg == "." || seg == "..":
			return fmt.Errorf("path segment %q", seg)
		case len(seg) > maxSegment:
			return fmt.Errorf("path segment longer than %d bytes", maxSegment)
		}
	}
	if !filepath.IsLocal(filepath.FromSlash(p)) {
		return errors.New("path outside the folder")
	}
	return nil
}

func (m *Manager) modelPresent(ctx context.Context, ws *os.Root, rel string, t task) bool {
	info, err := ws.Stat(filepath.FromSlash(rel))
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		return false
	}
	if t.size > 0 && info.Size() != t.size {
		return false
	}
	if t.sha256 == "" {
		return true
	}
	m.mu.Lock()
	known, ok := m.verified[t.target]
	m.mu.Unlock()
	if ok && known.size == info.Size() && known.modTime.Equal(info.ModTime()) && strings.EqualFold(known.sha256, t.sha256) {
		return true
	}
	f, err := ws.Open(filepath.FromSlash(rel))
	if err != nil {
		return false
	}
	sum, err := hashReader(ctx, f)
	f.Close()
	if err != nil || !strings.EqualFold(sum, t.sha256) {
		return false
	}
	m.remember(t.target, info, t.sha256)
	return true
}

func (m *Manager) remember(target string, info fs.FileInfo, sum string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.verified[target] = stamp{size: info.Size(), modTime: info.ModTime(), sha256: sum}
}

func (m *Manager) fetch(ctx context.Context, ws *os.Root, t task, onProgress func(float64)) error {
	if t.repo != nil {
		return m.clone(ctx, t)
	}
	if err := mkdirAll(ws, t.dir); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	dirName := t.dir
	if dirName == "" {
		dirName = "."
	}
	dir, err := ws.OpenRoot(filepath.FromSlash(dirName))
	if err != nil {
		return fmt.Errorf("open folder: %w", err)
	}
	defer dir.Close()

	part := partName(t.base)
	f, err := dir.OpenFile(part, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	err = m.fill(ctx, f, t, onProgress)
	if closeErr := f.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close: %w", closeErr)
	}
	if err != nil {
		_ = dir.Remove(part)
		return err
	}
	if err := renameIn(dir, part, t.base); err != nil {
		_ = dir.Remove(part)
		return fmt.Errorf("rename: %w", err)
	}
	if t.sha256 != "" {
		if info, err := dir.Stat(t.base); err == nil {
			m.remember(t.target, info, t.sha256)
		}
	}
	return nil
}

func (m *Manager) fill(ctx context.Context, f *os.File, t task, onProgress func(float64)) error {
	if t.inline != "" && t.url == "" {
		if _, err := f.WriteString(t.inline); err != nil {
			return fmt.Errorf("write inline: %w", err)
		}
		return nil
	}
	if err := m.download(ctx, t, f, onProgress); err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if t.size > 0 && info.Size() != t.size {
		return permanent("size %d, expected %d", info.Size(), t.size)
	}
	if t.model && info.Size() == 0 {
		return errors.New("empty file")
	}
	if t.sha256 != "" {
		sum, err := hashReader(ctx, io.NewSectionReader(f, 0, 1<<62))
		if err != nil {
			return fmt.Errorf("hash: %w", err)
		}
		if !strings.EqualFold(sum, t.sha256) {
			return permanent("sha256 mismatch (got %s)", sum[:12])
		}
	}
	return nil
}

func (m *Manager) download(ctx context.Context, t task, f *os.File, onProgress func(float64)) error {
	resp, err := Get(ctx, m.httpClient, t.url, "bytes=0-", m.idle)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	if isHTML(resp.Header.Get("Content-Type")) {
		resp.Body.Close()
		return permanent("got an html page instead of the file")
	}
	total := resp.ContentLength
	if resp.StatusCode == http.StatusPartialContent {
		total = totalFromContentRange(resp.Header.Get("Content-Range"))
	}
	if total > 0 {
		if t.size > 0 && total != t.size {
			resp.Body.Close()
			return permanent("size %d, expected %d", total, t.size)
		}
		if err := ensureSpace(f, total); err != nil {
			resp.Body.Close()
			return err
		}
	}
	if resp.StatusCode == http.StatusPartialContent && total >= m.rangeMin {
		final := resp.Request.URL.String()
		resp.Body.Close()
		return m.downloadRanged(ctx, final, f, total, onProgress)
	}

	defer resp.Body.Close()
	pw := &progressWriter{total: total, onProgress: onProgress, throttle: reportInterval}
	written, err := io.Copy(io.MultiWriter(f, pw), resp.Body)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	if total > 0 && written != total {
		return fmt.Errorf("download: got %d of %d bytes", written, total)
	}
	return nil
}

func isHTML(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/html")
	}
	return mediaType == "text/html"
}

func ensureSpace(f *os.File, need int64) error {
	var st unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &st); err != nil {
		return nil
	}
	free := st.Bavail * uint64(st.Bsize)
	if uint64(need) > free {
		return permanent("not enough disk space: need %d bytes, free %d", need, free)
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

func (m *Manager) downloadRanged(ctx context.Context, rawURL string, out *os.File, size int64, onProgress func(float64)) error {
	if err := out.Truncate(size); err != nil {
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
			if err := m.downloadPart(partCtx, rawURL, out, from, to, pw); err != nil {
				errs[idx] = err
				cancel()
			}
		}(i, start, end)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return fmt.Errorf("download: %w", e)
		}
	}
	return nil
}

func (m *Manager) downloadPart(ctx context.Context, rawURL string, out *os.File, from, to int64, pw *progressWriter) error {
	var lastErr error
	for attempt := 0; attempt < rangeAttempts; attempt++ {
		n, err := m.downloadRange(ctx, rawURL, out, from, to, pw)
		from += n
		if from > to {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if isPermanent(err) {
				return err
			}
			lastErr = err
			continue
		}
		lastErr = fmt.Errorf("short body: %d bytes missing", to-from+1)
	}
	return lastErr
}

func (m *Manager) downloadRange(ctx context.Context, rawURL string, out *os.File, from, to int64, pw *progressWriter) (int64, error) {
	resp, err := Get(ctx, m.httpClient, rawURL, fmt.Sprintf("bytes=%d-%d", from, to), m.idle)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("http %d for range %d-%d", resp.StatusCode, from, to)
	}
	return io.Copy(io.MultiWriter(io.NewOffsetWriter(out, from), pw), resp.Body)
}

func Get(ctx context.Context, c *http.Client, rawURL, rng string, idle time.Duration) (*http.Response, error) {
	reqCtx, watch := newIdleWatch(ctx, idle)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		watch.stop()
		return nil, Redact(err)
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := c.Do(req)
	if err != nil {
		watch.stop()
		return nil, Redact(watch.explain(err))
	}
	resp.Body = &idleBody{ReadCloser: resp.Body, watch: watch}
	return resp, nil
}

func Redact(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("%s: %w", urlErr.Op, urlErr.Err)
	}
	return err
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
		return fmt.Errorf("git clone: %w: %s", err, Clip(strings.TrimSpace(string(out)), maxItemError))
	}
	return nil
}

func (m *Manager) report(ctx context.Context, state string, progress *int, detail *string) {
	m.emit(ctx, client.AgentContentObserved{ObservedState: state, Progress: progress, Detail: detail})
}

func (m *Manager) emit(ctx context.Context, obs client.AgentContentObserved) {
	m.mu.Lock()
	m.observed = obs
	m.mu.Unlock()
	if m.reporter != nil {
		m.reporter(ctx, obs)
	}
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

func ready() client.AgentContentObserved {
	return client.AgentContentObserved{ObservedState: client.ContentReady, Progress: ptr(100)}
}

func errObs(msg string) client.AgentContentObserved {
	e := Clip(msg, maxError)
	return client.AgentContentObserved{ObservedState: client.ContentError, LastError: &e}
}

func regularFile(ws *os.Root, rel string) bool {
	info, err := ws.Stat(filepath.FromSlash(rel))
	return err == nil && info.Mode().IsRegular()
}

func dirExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

func mkdirAll(ws *os.Root, rel string) error {
	if rel == "" || rel == "." {
		return nil
	}
	current := ""
	for _, seg := range strings.Split(rel, "/") {
		current = path.Join(current, seg)
		if err := ws.Mkdir(filepath.FromSlash(current), dirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	return nil
}

func renameIn(dir *os.Root, from, to string) error {
	d, err := dir.Open(".")
	if err != nil {
		return err
	}
	defer d.Close()
	fd := int(d.Fd())
	return unix.Renameat(fd, from, fd, to)
}

func partName(base string) string {
	sum := sha256.Sum256([]byte(base))
	return ".yougpu-" + hex.EncodeToString(sum[:8]) + ".part"
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func CtxReader(ctx context.Context, r io.Reader) io.Reader {
	return ctxReader{ctx: ctx, r: r}
}

func hashReader(ctx context.Context, r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.CopyBuffer(h, CtxReader(ctx, r), make([]byte, hashBuffer)); err != nil {
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

func Clip(s string, n int) string {
	units := 0
	for i, r := range s {
		w := utf16.RuneLen(r)
		if w < 0 {
			w = 1
		}
		if units+w > n {
			return s[:i]
		}
		units += w
	}
	return s
}

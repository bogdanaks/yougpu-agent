package content

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const (
	WorkspaceContainerPath = "/workspace"
	dirPerm                = 0o777
	reportInterval         = 2 * time.Second
)

type Manager struct {
	httpClient *http.Client
	logger     *slog.Logger
	reporter   func(context.Context, client.AgentContentObserved)
}

func New(logger *slog.Logger) *Manager {
	return &Manager{
		// Без общего timeout: модели весят десятки ГБ, отмена идёт по ctx.
		httpClient: &http.Client{},
		logger:     logger,
	}
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
			return errObs(fmt.Sprintf("%s: %s", t.label, truncate(err.Error(), 200)))
		}
	}

	m.logger.Info("content ready", "items", total)
	return ready(nil)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.url, nil)
	if err != nil {
		return err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}

	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	hasher := sha256.New()
	pw := &progressWriter{total: resp.ContentLength, onProgress: onProgress, throttle: reportInterval}
	if _, err := io.Copy(io.MultiWriter(out, hasher, pw), resp.Body); err != nil {
		out.Close()
		os.Remove(tmp)
		return fmt.Errorf("download: %w", err)
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close: %w", err)
	}

	if t.sha256 != "" {
		sum := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(sum, t.sha256) {
			os.Remove(tmp)
			return fmt.Errorf("sha256 mismatch (got %s)", sum[:12])
		}
	}
	return os.Rename(tmp, t.target)
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
	if m.reporter == nil {
		return
	}
	m.reporter(ctx, client.AgentContentObserved{ObservedState: state, Progress: progress, Detail: detail})
}

type progressWriter struct {
	onProgress func(float64)
	total      int64 // Content-Length; <=0 если сервер не отдал
	read       int64
	last       time.Time
	throttle   time.Duration
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n := len(b)
	p.read += int64(n)
	if p.onProgress != nil && time.Since(p.last) >= p.throttle {
		p.last = time.Now()
		frac := 0.0
		if p.total > 0 {
			frac = float64(p.read) / float64(p.total)
		}
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

package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const (
	WorkspaceContainerPath = "/workspace"
	headerTimeout          = 2 * time.Minute
	maxRedirects           = 5
)

func NewHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = headerTimeout
	return &http.Client{Transport: transport, CheckRedirect: checkRedirect}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxRedirects {
		return fmt.Errorf("more than %d redirects", maxRedirects)
	}
	if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
		return errors.New("redirect from https to http")
	}
	req.Header.Del("Referer")
	return nil
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

func ClipTail(s string, n int) string {
	units := 0
	for i := len(s); i > 0; {
		r, size := utf8.DecodeLastRuneInString(s[:i])
		w := utf16.RuneLen(r)
		if w < 0 {
			w = 1
		}
		if units+w > n {
			return s[i:]
		}
		units += w
		i -= size
	}
	return s
}

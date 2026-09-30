package fetch

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func TestWorkspaceRoot(t *testing.T) {
	ws := &client.AgentContainerSpec{Volumes: []client.ContainerVolume{{Host: "/root/workspace", Container: WorkspaceContainerPath}}}
	if got := WorkspaceRoot(ws); got != "/root/workspace" {
		t.Fatalf("want /root/workspace, got %q", got)
	}
	if got := WorkspaceRoot(nil); got != "" {
		t.Fatalf("want empty for nil container, got %q", got)
	}
	noWs := &client.AgentContainerSpec{Volumes: []client.ContainerVolume{{Host: "/x", Container: "/data"}}}
	if got := WorkspaceRoot(noWs); got != "" {
		t.Fatalf("want empty when no /workspace volume, got %q", got)
	}
}

func TestClipKeepsWholeRunes(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"abc", 5, "abc"},
		{"абв", 2, "аб"},
		{"a😀b", 2, "a"},
		{"a😀b", 3, "a😀"},
	}
	for _, c := range cases {
		if got := Clip(c.in, c.n); got != c.want {
			t.Errorf("Clip(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

func TestClipTailKeepsWholeRunesFromTheEnd(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"abc", 5, "abc"},
		{"абв", 2, "бв"},
		{"a😀b", 2, "b"},
		{"a😀b", 3, "😀b"},
	}
	for _, c := range cases {
		if got := ClipTail(c.in, c.n); got != c.want {
			t.Errorf("ClipTail(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

func chain(t *testing.T, hops int, final http.HandlerFunc) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if n < hops {
			http.Redirect(w, r, srv.URL+"/"+strconv.Itoa(n+1), http.StatusFound)
			return
		}
		final(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRedirectsAreLimitedToFive(t *testing.T) {
	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }

	resp, err := Get(context.Background(), NewHTTPClient(), chain(t, 5, ok).URL+"/0", "", time.Minute)
	if err != nil {
		t.Fatalf("five redirects must be followed: %v", err)
	}
	resp.Body.Close()

	if resp, err := Get(context.Background(), NewHTTPClient(), chain(t, 6, ok).URL+"/0", "", time.Minute); err == nil {
		resp.Body.Close()
		t.Fatal("sixth redirect must be refused")
	}
}

func TestRedirectDoesNotSendReferer(t *testing.T) {
	var referer atomic.Value
	referer.Store("unset")
	srv := chain(t, 1, func(w http.ResponseWriter, r *http.Request) {
		referer.Store(r.Header.Get("Referer"))
	})

	resp, err := Get(context.Background(), NewHTTPClient(), srv.URL+"/0?X-Amz-Signature=SECRET", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := referer.Load().(string); got != "" {
		t.Fatalf("referer leaked to the redirect target: %q", got)
	}
}

func TestRedirectFromHTTPSToHTTPIsRefused(t *testing.T) {
	var plainHits atomic.Int32
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		plainHits.Add(1)
	}))
	defer plain.Close()
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/state", http.StatusFound)
	}))
	defer secure.Close()
	c := NewHTTPClient()
	c.Transport.(*http.Transport).TLSClientConfig = secure.Client().Transport.(*http.Transport).TLSClientConfig

	resp, err := Get(context.Background(), c, secure.URL+"/state", "", time.Minute)
	if err == nil {
		resp.Body.Close()
		t.Fatal("downgrade to http must be refused")
	}
	if plainHits.Load() != 0 {
		t.Fatal("request reached the http target")
	}
}

type stingyListener struct{ net.Listener }

func (l stingyListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		_ = c.(*net.TCPConn).SetReadBuffer(4096)
	}
	return c, err
}

func stingyClient() *http.Client {
	c := NewHTTPClient()
	c.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err == nil {
			_ = conn.(*net.TCPConn).SetWriteBuffer(4096)
		}
		return conn, err
	}
	return c
}

func stalledServer(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	srv.Listener = stingyListener{srv.Listener}
	srv.Start()
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv
}

func TestPutGivesUpWhenNothingIsSent(t *testing.T) {
	srv := stalledServer(t)
	body := make([]byte, 8<<20)

	start := time.Now()
	resp, err := Put(context.Background(), stingyClient(), srv.URL+"/b2?X-Amz-Signature=SECRET", bytes.NewReader(body), int64(len(body)), 100*time.Millisecond)

	if err == nil {
		resp.Body.Close()
		t.Fatal("stalled upload must fail")
	}
	if !strings.Contains(err.Error(), "nothing sent for 100ms") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("stalled upload took %s to give up", took)
	}
}

func TestPutSendsTheWholeBodyWithItsLength(t *testing.T) {
	var got atomic.Int64
	var length atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		length.Store(r.ContentLength)
		n, _ := io.Copy(io.Discard, r.Body)
		got.Store(n)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	body := make([]byte, 1<<20)

	resp, err := Put(context.Background(), NewHTTPClient(), srv.URL, bytes.NewReader(body), int64(len(body)), 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK || got.Load() != int64(len(body)) || length.Load() != int64(len(body)) {
		t.Fatalf("status %d, sent %d with length %d, want %d", resp.StatusCode, got.Load(), length.Load(), len(body))
	}
}

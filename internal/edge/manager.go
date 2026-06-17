package edge

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const (
	edgeListenAddr      = ":443"
	edgeShutdownTimeout = 5 * time.Second
)

type route struct {
	endpoint client.AccessEndpoint
	proxy    *httputil.ReverseProxy
}

type Manager struct {
	log *slog.Logger

	mu     sync.Mutex
	hash   string
	server *http.Server
	done   chan struct{}
	ready  atomic.Bool
}

func NewManager(log *slog.Logger) *Manager {
	return &Manager{log: log}
}

func (m *Manager) Reconcile(ctx context.Context, spec *client.AgentAccessSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if spec == nil || len(spec.Endpoints) == 0 {
		if m.server != nil {
			m.log.Info("edge stopping", "reason", "no spec")
			m.stopLocked()
		}
		return
	}
	if spec.CertPEM == "" || spec.KeyPEM == "" {
		if m.server != nil {
			m.log.Info("edge stopping", "reason", "cert missing")
			m.stopLocked()
		}
		m.log.Warn("edge not started: cert missing in spec")
		return
	}

	desired := specHash(spec)
	if m.server != nil && desired == m.hash {
		return
	}
	if m.server != nil {
		m.log.Info("edge restarting", "reason", "spec changed")
		m.stopLocked()
	}
	if err := m.startLocked(ctx, spec, desired); err != nil {
		m.log.Error("edge start failed", "err", err)
	}
}

func (m *Manager) Ready() bool {
	return m.ready.Load()
}

func (m *Manager) stopLocked() {
	if m.done != nil {
		close(m.done)
		m.done = nil
	}
	if m.server != nil {
		sctx, cancel := context.WithTimeout(context.Background(), edgeShutdownTimeout)
		_ = m.server.Shutdown(sctx)
		cancel()
	}
	m.server = nil
	m.hash = ""
	m.ready.Store(false)
}

func (m *Manager) startLocked(ctx context.Context, spec *client.AgentAccessSpec, hash string) error {
	cert, err := tls.X509KeyPair([]byte(spec.CertPEM), []byte(spec.KeyPEM))
	if err != nil {
		return fmt.Errorf("load cert: %w", err)
	}
	routes, err := buildRoutes(spec)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", edgeListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", edgeListenAddr, err)
	}

	server := &http.Server{
		Handler:   m.handler(routes, spec.Token, strings.Join(spec.FrameAncestors, " "), newNonceCache()),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}
	done := make(chan struct{})

	m.server = server
	m.done = done
	m.hash = hash
	m.ready.Store(true)

	go func() {
		if serveErr := server.ServeTLS(ln, "", ""); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			m.log.Warn("edge server exited", "err", serveErr)
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), edgeShutdownTimeout)
			_ = server.Shutdown(sctx)
			cancel()
		case <-done:
		}
	}()

	m.log.Info("edge started", "addr", edgeListenAddr, "endpoints", len(routes))
	return nil
}

func (m *Manager) handler(routes map[string]route, token, frameAncestors string, nonces *nonceCache) http.Handler {
	cookieVal := cookieValue(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rt, ok := routes[hostLabel(r.Host)]
		if !ok {
			http.Error(w, "unknown endpoint", http.StatusNotFound)
			return
		}
		if cap := r.URL.Query().Get("__bootstrap"); cap != "" {
			m.bootstrap(w, r, token, cookieVal, nonces)
			return
		}
		if !authorized(rt.endpoint, token, cookieVal, r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		rt.proxy.ServeHTTP(w, r)
	})
}

// Бутстрап браузерной куки: проверяем одноразовую capability (HMAC от K + exp + nonce-once),
// ставим CHIPS-куку и редиректим на чистый URL без __bootstrap. Невалидный cap → 401.
func (m *Manager) bootstrap(w http.ResponseWriter, r *http.Request, token, cookieVal string, nonces *nonceCache) {
	now := time.Now().Unix()
	nonce, exp, ok := parseCap(token, r.URL.Query().Get("__bootstrap"), now)
	if !ok || !nonces.checkAndMark(nonce, exp, now) {
		http.Error(w, "invalid bootstrap", http.StatusUnauthorized)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:        "yougpu_access",
		Value:       cookieVal,
		Path:        "/",
		HttpOnly:    true,
		Secure:      true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	})
	q := r.URL.Query()
	q.Del("__bootstrap")
	r.URL.RawQuery = q.Encode()
	http.Redirect(w, r, r.URL.RequestURI(), http.StatusFound)
}

func authorized(ep client.AccessEndpoint, token, cookieVal string, r *http.Request) bool {
	if ep.Visibility == client.VisibilityPublic {
		return true
	}
	const prefix = "Bearer "
	if header := r.Header.Get("Authorization"); strings.HasPrefix(header, prefix) {
		if subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(token)) == 1 {
			return true
		}
	}
	if c, err := r.Cookie("yougpu_access"); err == nil {
		return subtle.ConstantTimeCompare([]byte(c.Value), []byte(cookieVal)) == 1
	}
	return false
}

// Значение браузерной куки = HMAC(K, "yougpu_access") — производное, сырой K в браузер не попадает.
func cookieValue(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte("yougpu_access"))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// cap = base64url(JSON{exp,nonce}).base64url(HMAC(K, payload)). Возвращает nonce и exp при валидной подписи.
func parseCap(token, cap string, now int64) (string, int64, bool) {
	dot := strings.LastIndexByte(cap, '.')
	if dot <= 0 {
		return "", 0, false
	}
	payload, sig := cap[:dot], cap[dot+1:]
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(payload))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(sig), []byte(expected)) != 1 {
		return "", 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return "", 0, false
	}
	var claims struct {
		Exp   int64  `json:"exp"`
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return "", 0, false
	}
	if claims.Exp < now || claims.Nonce == "" {
		return "", 0, false
	}
	return claims.Nonce, claims.Exp, true
}

type nonceCache struct {
	mu   sync.Mutex
	used map[string]int64
}

func newNonceCache() *nonceCache {
	return &nonceCache{used: map[string]int64{}}
}

func (n *nonceCache) checkAndMark(nonce string, exp, now int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, e := range n.used {
		if e < now {
			delete(n.used, k)
		}
	}
	if _, seen := n.used[nonce]; seen {
		return false
	}
	n.used[nonce] = exp
	return true
}

func buildRoutes(spec *client.AgentAccessSpec) (map[string]route, error) {
	var csp string
	if len(spec.FrameAncestors) > 0 {
		csp = "frame-ancestors " + strings.Join(spec.FrameAncestors, " ")
	}
	routes := make(map[string]route, len(spec.Endpoints))
	for _, ep := range spec.Endpoints {
		if ep.Port <= 0 {
			return nil, fmt.Errorf("endpoint %q: invalid port %d", ep.Key, ep.Port)
		}
		target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(ep.Port))}
		proxy := httputil.NewSingleHostReverseProxy(target)
		if csp != "" {
			proxy.ModifyResponse = func(resp *http.Response) error {
				resp.Header.Del("X-Frame-Options")
				resp.Header.Set("Content-Security-Policy", csp)
				return nil
			}
		}
		routes[spec.Slug+"-"+ep.Key] = route{endpoint: ep, proxy: proxy}
	}
	return routes, nil
}

func hostLabel(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if i := strings.IndexByte(host, '.'); i >= 0 {
		return host[:i]
	}
	return host
}

func specHash(spec *client.AgentAccessSpec) string {
	raw, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return strconv.FormatUint(h.Sum64(), 16)
}

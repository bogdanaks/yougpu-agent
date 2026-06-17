package edge

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func testManager() *Manager {
	return NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestHostLabel(t *testing.T) {
	cases := map[string]string{
		"k7m2p9qx-webui.app.yougpu.ru":     "k7m2p9qx-webui",
		"k7m2p9qx-webui.app.yougpu.ru:443": "k7m2p9qx-webui",
		"single":                           "single",
	}
	for in, want := range cases {
		if got := hostLabel(in); got != want {
			t.Fatalf("hostLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAuthorized(t *testing.T) {
	pub := client.AccessEndpoint{Key: "api", Port: 1, Visibility: client.VisibilityPublic}
	priv := client.AccessEndpoint{Key: "webui", Port: 2, Visibility: client.VisibilityPrivate}
	cookieVal := cookieValue("tok")

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if !authorized(pub, "tok", cookieVal, r) {
		t.Fatal("public must pass without creds")
	}
	if authorized(priv, "tok", cookieVal, r) {
		t.Fatal("private must reject without creds")
	}
	r.Header.Set("Authorization", "Bearer wrong")
	if authorized(priv, "tok", cookieVal, r) {
		t.Fatal("private must reject wrong bearer")
	}
	r.Header.Set("Authorization", "Bearer tok")
	if !authorized(priv, "tok", cookieVal, r) {
		t.Fatal("private must accept matching bearer (raw K)")
	}

	rc := httptest.NewRequest(http.MethodGet, "/", nil)
	rc.AddCookie(&http.Cookie{Name: "yougpu_access", Value: cookieVal})
	if !authorized(priv, "tok", cookieVal, rc) {
		t.Fatal("private must accept matching cookie")
	}
	rc2 := httptest.NewRequest(http.MethodGet, "/", nil)
	rc2.AddCookie(&http.Cookie{Name: "yougpu_access", Value: "wrong"})
	if authorized(priv, "tok", cookieVal, rc2) {
		t.Fatal("private must reject wrong cookie")
	}
}

func makeCap(token string, exp int64, nonce string) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":` + strconv.FormatInt(exp, 10) + `,"nonce":"` + nonce + `"}`))
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestParseCap(t *testing.T) {
	now := int64(1_700_000_000)
	nonce, exp, ok := parseCap("tok", makeCap("tok", now+60, "n1"), now)
	if !ok || nonce != "n1" || exp != now+60 {
		t.Fatalf("valid cap rejected: ok=%v nonce=%q exp=%d", ok, nonce, exp)
	}
	if _, _, ok := parseCap("tok", makeCap("tok", now-1, "n1"), now); ok {
		t.Fatal("expired cap must be rejected")
	}
	if _, _, ok := parseCap("other", makeCap("tok", now+60, "n1"), now); ok {
		t.Fatal("cap signed with different K must be rejected")
	}
	if _, _, ok := parseCap("tok", "garbage", now); ok {
		t.Fatal("garbage cap must be rejected")
	}
}

func TestNonceCacheOnce(t *testing.T) {
	n := newNonceCache()
	now := int64(1_700_000_000)
	if !n.checkAndMark("n1", now+60, now) {
		t.Fatal("first use must pass")
	}
	if n.checkAndMark("n1", now+60, now) {
		t.Fatal("replay must be rejected")
	}
}

func TestBootstrapSetsCookieAndRedirects(t *testing.T) {
	spec := &client.AgentAccessSpec{
		Slug:      "k7m2p9qx",
		Token:     "tok",
		Endpoints: []client.AccessEndpoint{{Key: "webui", Port: 1, Visibility: client.VisibilityPrivate}},
	}
	routes, err := buildRoutes(spec)
	if err != nil {
		t.Fatalf("buildRoutes: %v", err)
	}
	edge := httptest.NewServer(testManager().handler(routes, spec.Token, "", newNonceCache()))
	defer edge.Close()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	cap := makeCap("tok", time.Now().Unix()+60, "boot1")
	req, _ := http.NewRequest(http.MethodGet, edge.URL+"/?__bootstrap="+cap, nil)
	req.Host = "k7m2p9qx-webui"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("want 302, got %d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); strings.Contains(loc, "__bootstrap") {
		t.Fatalf("redirect must drop __bootstrap, got %q", loc)
	}
	setCookie := resp.Header.Get("Set-Cookie")
	if !strings.Contains(setCookie, "yougpu_access=") || !strings.Contains(setCookie, "Partitioned") || !strings.Contains(setCookie, "SameSite=None") {
		t.Fatalf("cookie must be CHIPS (Partitioned; SameSite=None), got %q", setCookie)
	}
}

func TestBuildRoutesInvalidPort(t *testing.T) {
	spec := &client.AgentAccessSpec{Slug: "s", Endpoints: []client.AccessEndpoint{{Key: "webui", Port: 0}}}
	if _, err := buildRoutes(spec); err == nil {
		t.Fatal("expected error for invalid port")
	}
}

func TestHandlerRouting(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	defer upstream.Close()

	u, _ := url.Parse(upstream.URL)
	port, _ := strconv.Atoi(u.Port())

	spec := &client.AgentAccessSpec{
		Slug:  "k7m2p9qx",
		Token: "tok",
		Endpoints: []client.AccessEndpoint{
			{Key: "webui", Port: port, Visibility: client.VisibilityPrivate},
			{Key: "api", Port: port, Visibility: client.VisibilityPublic},
		},
	}
	routes, err := buildRoutes(spec)
	if err != nil {
		t.Fatalf("buildRoutes: %v", err)
	}
	edge := httptest.NewServer(testManager().handler(routes, spec.Token, "", newNonceCache()))
	defer edge.Close()

	do := func(label, auth string) int {
		req, _ := http.NewRequest(http.MethodGet, edge.URL+"/x", nil)
		req.Host = label
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	if code := do("k7m2p9qx-webui", "Bearer tok"); code != http.StatusOK {
		t.Fatalf("private+bearer = %d, want 200", code)
	}
	if code := do("k7m2p9qx-webui", ""); code != http.StatusUnauthorized {
		t.Fatalf("private no-auth = %d, want 401", code)
	}
	if code := do("k7m2p9qx-api", ""); code != http.StatusOK {
		t.Fatalf("public = %d, want 200", code)
	}
	if code := do("unknown-label", "Bearer tok"); code != http.StatusNotFound {
		t.Fatalf("unknown host = %d, want 404", code)
	}
}

func TestSpecHash(t *testing.T) {
	a := &client.AgentAccessSpec{Slug: "s", Token: "t", Endpoints: []client.AccessEndpoint{{Key: "webui", Port: 80}}}
	b := &client.AgentAccessSpec{Slug: "s", Token: "t", Endpoints: []client.AccessEndpoint{{Key: "webui", Port: 81}}}
	if specHash(a) != specHash(a) {
		t.Fatal("hash must be stable")
	}
	if specHash(a) == specHash(b) {
		t.Fatal("hash must differ when port differs")
	}
}

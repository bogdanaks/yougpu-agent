package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	tokenHeader      = "x-provisioning-token"
	defaultTimeout   = 30 * time.Second
	maxResponseBytes = 1 << 20
	// На 429 ждём Retry-After (с cap'ом против мусора в header'е) и делаем один retry.
	rateLimitMaxWait      = 60 * time.Second
	rateLimitFallbackWait = 5 * time.Second
)

type Client struct {
	baseURL string
	token   string
	version string
	http    *http.Client
	log     *slog.Logger
}

func New(baseURL, token, version string, log *slog.Logger) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		version: version,
		http:    &http.Client{Timeout: defaultTimeout},
		log:     log,
	}
}

// StreamSpec открывает SSE-стрим и шлёт каждое spec-событие в out-канал. Возвращается
// когда соединение закрылось / контекст отменён / backend ответил терминальной ошибкой.
// На 410/401 возвращается HTTPError — caller должен self-stop. На сетевые ошибки —
// caller реконнектит с backoff'ом.
func (c *Client) StreamSpec(ctx context.Context, out chan<- *AgentSpec) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/spec/stream", nil)
	if err != nil {
		return err
	}
	req.Header.Set(tokenHeader, c.token)
	req.Header.Set("Accept", "text/event-stream")

	// Stream-клиент без global timeout — соединение долгоживущее.
	streamHTTP := &http.Client{Timeout: 0}
	resp, err := streamHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &HTTPError{Status: resp.StatusCode, Body: string(snippet)}
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxResponseBytes)
	var event, data string
	flush := func() {
		if event == "spec" && data != "" {
			var spec AgentSpec
			if err := json.Unmarshal([]byte(data), &spec); err == nil {
				select {
				case out <- &spec:
				case <-ctx.Done():
				}
			} else {
				c.log.Warn("invalid spec payload in SSE", "err", err)
			}
		}
		event, data = "", ""
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		case strings.HasPrefix(line, ":"):
			// SSE comment — игнорируем (heartbeat / keep-alive).
		}
	}
	return scanner.Err()
}

func (c *Client) PostStatus(ctx context.Context, status *AgentStatus) error {
	if status.AgentVersion == "" {
		status.AgentVersion = c.version
	}
	if err := c.do(ctx, http.MethodPost, "/status", status, nil); err != nil {
		return fmt.Errorf("post status: %w", err)
	}
	return nil
}

// Heartbeat — lightweight pulse: только last_status_at + сбрасывает agent_unresponsive_since.
// Bcakend возвращает 410 на терминальных инстансах.
func (c *Client) Heartbeat(ctx context.Context) error {
	if err := c.do(ctx, http.MethodPut, "/heartbeat", nil, nil); err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// IsGone проверяет, что backend ответил 410 (инстанс терминальный) — агент должен self-stop.
func IsGone(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Status == http.StatusGone
	}
	return false
}

func (c *Client) RotateStorageKeys(ctx context.Context) (*RotateStorageKeysResponse, error) {
	var resp RotateStorageKeysResponse
	if err := c.do(ctx, http.MethodPost, "/rotate-storage-keys", nil, &resp); err != nil {
		return nil, fmt.Errorf("rotate storage keys: %w", err)
	}
	if resp.AccessKey == "" {
		return nil, errors.New("rotate storage keys: empty access key in response")
	}
	return &resp, nil
}

func (c *Client) ReportProvisioningStatus(ctx context.Context, req *ProvisioningStatusRequest) error {
	if err := c.do(ctx, http.MethodPost, "/provisioning-status", req, nil); err != nil {
		return fmt.Errorf("report provisioning status: %w", err)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	// buf нужен отдельно от reader'а: bytes.NewReader исчерпывается после первого Read,
	// а на 429-retry нужно отправить тело ещё раз.
	var buf []byte
	if body != nil {
		var err error
		buf, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
	}

	send := func() (*http.Response, error) {
		var reader io.Reader
		if buf != nil {
			reader = bytes.NewReader(buf)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
		if err != nil {
			return nil, err
		}
		req.Header.Set(tokenHeader, c.token)
		req.Header.Set("Accept", "application/json")
		if buf != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		return c.http.Do(req)
	}

	resp, err := send()
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		wait := parseRetryAfter(resp.Header.Get("Retry-After"))
		resp.Body.Close()
		c.log.Warn("rate limited by backend", "path", path, "wait", wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		resp, err = send()
		if err != nil {
			return err
		}
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &HTTPError{Status: resp.StatusCode, Body: string(snippet)}
	}

	if out == nil {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	var env struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return json.Unmarshal(raw, out)
	}
	if len(env.Data) == 0 {
		return json.Unmarshal(raw, out)
	}
	return json.Unmarshal(env.Data, out)
}

// RFC 7231: число секунд ИЛИ HTTP-date. Пустой/битый header → fallback, > cap → cap.
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return rateLimitFallbackWait
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		d := time.Duration(secs) * time.Second
		if d > rateLimitMaxWait {
			return rateLimitMaxWait
		}
		return d
	}
	if t, err := http.ParseTime(h); err == nil {
		d := time.Until(t)
		if d <= 0 {
			return rateLimitFallbackWait
		}
		if d > rateLimitMaxWait {
			return rateLimitMaxWait
		}
		return d
	}
	return rateLimitFallbackWait
}

type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http %d: %s", e.Status, e.Body)
}

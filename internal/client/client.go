package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	tokenHeader      = "x-provisioning-token"
	defaultTimeout   = 30 * time.Second
	maxResponseBytes = 1 << 20
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

func (c *Client) GetSpec(ctx context.Context) (*AgentSpec, error) {
	var spec AgentSpec
	if err := c.do(ctx, http.MethodGet, "/spec", nil, &spec); err != nil {
		return nil, fmt.Errorf("get spec: %w", err)
	}
	return &spec, nil
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
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set(tokenHeader, c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
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

type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http %d: %s", e.Status, e.Body)
}

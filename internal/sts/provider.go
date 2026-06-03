package sts

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

type Applier interface {
	ApplyCredentials(ctx context.Context, creds *client.StorageCredentials) error
}

type Provider struct {
	httpClient       *client.Client
	applier          Applier
	log              *slog.Logger
	refreshThreshold time.Duration
	periodicInterval time.Duration

	mu      sync.Mutex
	current *client.StorageCredentials
}

func NewProvider(c *client.Client, applier Applier, log *slog.Logger, refreshThreshold, periodicInterval time.Duration) *Provider {
	return &Provider{
		httpClient:       c,
		applier:          applier,
		log:              log,
		refreshThreshold: refreshThreshold,
		periodicInterval: periodicInterval,
	}
}

func (p *Provider) EnsureFresh(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current != nil && time.Until(p.current.ExpiresAt) > p.refreshThreshold {
		return nil
	}
	return p.refreshLocked(ctx)
}

func (p *Provider) ForceRefresh(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refreshLocked(ctx)
}

func (p *Provider) Current() *client.StorageCredentials {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current == nil {
		return nil
	}
	c := *p.current
	return &c
}

func (p *Provider) Run(ctx context.Context) {
	t := time.NewTicker(p.periodicInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.EnsureFresh(ctx); err != nil {
				p.log.Error("background refresh failed", "err", err)
			}
		}
	}
}

func (p *Provider) refreshLocked(ctx context.Context) error {
	fresh, err := p.httpClient.GetStorageCredentials(ctx)
	if err != nil {
		return fmt.Errorf("fetch credentials: %w", err)
	}
	if p.current != nil && p.current.CredentialID == fresh.CredentialID {
		p.log.Debug("credentials unchanged, skipping apply", "credential_id", fresh.CredentialID)
		return nil
	}
	if err := p.applier.ApplyCredentials(ctx, fresh); err != nil {
		return fmt.Errorf("apply credentials: %w", err)
	}
	p.current = fresh
	p.log.Info("credentials applied",
		"credential_id", fresh.CredentialID,
		"expires_at", fresh.ExpiresAt.Format(time.RFC3339),
		"ttl_until_refresh", time.Until(fresh.ExpiresAt)-p.refreshThreshold,
	)
	return nil
}

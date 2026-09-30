package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/fetch"
)

type checkpointJob struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (j *checkpointJob) running() bool {
	select {
	case <-j.done:
		return false
	default:
		return true
	}
}

func (m *Manager) Checkpoint(ctx context.Context, spec *client.AgentStateSpec, container *client.AgentContainerSpec) {
	if spec == nil || spec.Checkpoint == nil || !m.packable() {
		return
	}
	root := fetch.WorkspaceRoot(container)
	if root == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealed || m.checkpoint != nil && m.checkpoint.running() {
		return
	}
	now := m.now()
	if m.checkpointAt.IsZero() {
		m.checkpointAt = now
		return
	}
	if now.Sub(m.checkpointAt) < time.Duration(spec.Checkpoint.EverySec)*time.Second {
		return
	}
	jobCtx, cancel := context.WithTimeout(ctx, m.checkpointTimeout)
	j := &checkpointJob{cancel: cancel, done: make(chan struct{})}
	m.checkpoint = j
	go m.runCheckpoint(jobCtx, j, *spec.Checkpoint, spec.Exclude, root)
}

func (m *Manager) runCheckpoint(ctx context.Context, j *checkpointJob, spec client.StateCheckpoint, exclude []string, root string) {
	defer close(j.done)
	defer j.cancel()
	err := m.checkpointOnce(ctx, spec, exclude, root)
	m.mu.Lock()
	m.checkpointAt = m.now()
	m.mu.Unlock()
	switch {
	case err == nil:
	case errors.Is(ctx.Err(), context.Canceled):
		m.log.Info("workspace checkpoint stopped")
	default:
		m.log.Warn("workspace checkpoint failed, will try on the next interval", "err", fetch.Clip(fetch.Redact(err).Error(), maxError))
	}
}

func (m *Manager) checkpointOnce(ctx context.Context, spec client.StateCheckpoint, exclude []string, root string) error {
	archive := m.marker(checkpointArchive)
	defer os.Remove(archive)
	limit := spec.MaxBytes
	if limit <= 0 {
		limit = m.maxUpload
	}
	sum, size, err := m.packToFile(ctx, root, spec.Include, exclude, archive, limit)
	if errors.Is(err, errTooLarge) {
		m.log.Warn("workspace checkpoint skipped until the next interval", "err", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("checkpoint pack: %w", err)
	}
	m.mu.Lock()
	unchanged := m.checkpointSum == sum
	m.mu.Unlock()
	if unchanged {
		m.log.Debug("workspace checkpoint unchanged")
		return nil
	}
	target, err := m.backend.CheckpointURL(ctx, client.CheckpointRequest{SizeBytes: size, SHA256: sum})
	if client.IsConflict(err) {
		m.log.Info("backend takes no workspace checkpoint now")
		return nil
	}
	if client.IsBadRequest(err) {
		m.log.Warn("backend refused the workspace checkpoint, will try on the next interval", "bytes", size, "err", fetch.Clip(err.Error(), maxError))
		return nil
	}
	if err != nil {
		return err
	}
	if err := m.upload(ctx, target.UploadURL, archive, size); err != nil {
		return err
	}
	err = m.backend.CommitCheckpoint(ctx, client.CheckpointCommit{Key: target.Key, SHA256: sum, SizeBytes: size})
	if client.IsConflict(err) {
		m.log.Info("backend does not need this workspace checkpoint")
		return nil
	}
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.checkpointSum = sum
	m.mu.Unlock()
	m.log.Info("workspace checkpoint saved", "bytes", size)
	return nil
}

func (m *Manager) stopCheckpoints() {
	m.mu.Lock()
	m.sealed = true
	j := m.checkpoint
	m.mu.Unlock()
	if j == nil {
		return
	}
	j.cancel()
	<-j.done
}

package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

type confSection struct {
	name string
	body string
}

func remoteName(driveID string) string { return remotePrefix + driveID }

func (m *Manager) ensureRemote(ctx context.Context, driveID string) error {
	if !driveIDPattern.MatchString(driveID) {
		return fmt.Errorf("invalid drive id %q", driveID)
	}
	m.confMu.Lock()
	sections, err := m.readConf()
	m.confMu.Unlock()
	if err != nil {
		return fmt.Errorf("read rclone config: %w", err)
	}
	if findSection(sections, remoteName(driveID)) >= 0 {
		return nil
	}
	return m.renewRemote(ctx, driveID)
}

func (m *Manager) keyRetryDue(driveID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, ok := m.keyAskedAt[driveID]
	return !ok || m.now().Sub(at) >= keyRetryEvery
}

func (m *Manager) renewRemote(ctx context.Context, driveID string) error {
	m.mu.Lock()
	m.keyAskedAt[driveID] = m.now()
	m.mu.Unlock()
	creds, err := m.creds.GetStorageCredentials(ctx, driveID)
	if err != nil {
		return fmt.Errorf("storage key: %w", err)
	}
	if err := m.putRemote(driveID, creds); err != nil {
		return fmt.Errorf("write rclone config: %w", err)
	}
	m.log.Info("storage key written", "id", driveID, "credential_id", creds.CredentialID)
	return nil
}

func (m *Manager) putRemote(driveID string, c *client.StorageCredentials) error {
	for _, v := range []string{c.AccessKey, c.SecretKey, c.Endpoint} {
		if strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("storage key of %s has a malformed value", driveID)
		}
	}
	name := remoteName(driveID)
	section := confSection{name: name, body: fmt.Sprintf(`[%s]
type = s3
provider = Other
env_auth = false
access_key_id = %s
secret_access_key = %s
endpoint = %s
force_path_style = false
acl = private
`, name, c.AccessKey, c.SecretKey, c.Endpoint)}

	m.confMu.Lock()
	defer m.confMu.Unlock()
	sections, err := m.readConf()
	if err != nil {
		return err
	}
	if i := findSection(sections, name); i >= 0 {
		sections[i] = section
	} else {
		sections = append(sections, section)
	}
	return m.writeConf(sections)
}

func (m *Manager) dropRemote(driveID string) error {
	name := remoteName(driveID)
	m.confMu.Lock()
	defer m.confMu.Unlock()
	sections, err := m.readConf()
	if err != nil {
		return err
	}
	i := findSection(sections, name)
	if i < 0 {
		return nil
	}
	m.mu.Lock()
	delete(m.keyAskedAt, driveID)
	m.mu.Unlock()
	return m.writeConf(append(sections[:i], sections[i+1:]...))
}

func (m *Manager) readConf() ([]confSection, error) {
	raw, err := os.ReadFile(m.rcloneConfigPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sections []confSection
	current := confSection{}
	flush := func() {
		if current.name != "" || strings.TrimSpace(current.body) != "" {
			sections = append(sections, current)
		}
	}
	for _, line := range strings.SplitAfter(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			flush()
			current = confSection{name: strings.TrimSpace(trimmed[1 : len(trimmed)-1])}
		}
		current.body += line
	}
	flush()
	return sections, nil
}

func (m *Manager) writeConf(sections []confSection) error {
	var b strings.Builder
	for i, s := range sections {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(strings.TrimRight(s.body, "\n"))
		b.WriteString("\n")
	}
	if err := os.MkdirAll(filepath.Dir(m.rcloneConfigPath), 0o700); err != nil {
		return err
	}
	tmp := m.rcloneConfigPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, configFileMode)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(b.String()); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(configFileMode); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, m.rcloneConfigPath)
}

func findSection(sections []confSection, name string) int {
	for i, s := range sections {
		if s.name == name {
			return i
		}
	}
	return -1
}

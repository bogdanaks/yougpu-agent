package sshkeys

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const (
	beginMarker = "# yougpu-managed-keys begin"
	endMarker   = "# yougpu-managed-keys end"
)

type Account struct {
	Home string
	UID  int
	GID  int
}

type Manager struct {
	lookup func(name string) (Account, error)
	log    *slog.Logger
}

func NewManager(log *slog.Logger) *Manager {
	return &Manager{lookup: lookupAccount, log: log}
}

func (m *Manager) SetLookupForTest(lookup func(name string) (Account, error)) {
	m.lookup = lookup
}

func (m *Manager) Reconcile(spec *client.AgentSSHSpec) error {
	if spec == nil || spec.User == "" {
		return nil
	}
	account, err := m.lookup(spec.User)
	if err != nil {
		return fmt.Errorf("lookup %s: %w", spec.User, err)
	}

	dir := filepath.Join(account.Home, ".ssh")
	path := filepath.Join(dir, "authorized_keys")
	current, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	keys := validKeys(spec.AuthorizedKeys)
	next := render(withoutBlock(string(current)), keys)
	if next == string(current) {
		return nil
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chown(dir, account.UID, account.GID); err != nil {
		return err
	}
	tmp := path + ".yougpu-tmp"
	if err := os.WriteFile(tmp, []byte(next), 0o600); err != nil {
		return err
	}
	if err := os.Chown(tmp, account.UID, account.GID); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	m.log.Info("ssh authorized keys applied", "user", spec.User, "keys", len(keys))
	return nil
}

func validKeys(keys []string) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.ContainsAny(key, "\r\n") {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" || strings.HasPrefix(key, "#") {
			continue
		}
		out = append(out, key)
	}
	return out
}

func withoutBlock(content string) string {
	var kept []string
	inside := false
	for _, line := range strings.Split(content, "\n") {
		switch {
		case line == beginMarker:
			inside = true
		case line == endMarker && inside:
			inside = false
		case !inside:
			kept = append(kept, line)
		}
	}
	return strings.TrimRight(strings.Join(kept, "\n"), "\n")
}

func render(base string, keys []string) string {
	var b strings.Builder
	if base != "" {
		b.WriteString(base)
		b.WriteString("\n")
	}
	if len(keys) == 0 {
		return b.String()
	}
	b.WriteString(beginMarker + "\n")
	for _, key := range keys {
		b.WriteString(key + "\n")
	}
	b.WriteString(endMarker + "\n")
	return b.String()
}

func lookupAccount(name string) (Account, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return Account{}, err
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return Account{}, err
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return Account{}, err
	}
	return Account{Home: u.HomeDir, UID: uid, GID: gid}, nil
}

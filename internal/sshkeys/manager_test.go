package sshkeys

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

const providerKey = "ssh-ed25519 AAAAprovider provider@hyperstack"

func newManager(t *testing.T) (*Manager, string) {
	t.Helper()
	home := t.TempDir()
	m := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.SetLookupForTest(func(name string) (Account, error) {
		if name != "ubuntu" {
			return Account{}, errors.New("unknown user " + name)
		}
		return Account{Home: home, UID: os.Getuid(), GID: os.Getgid()}, nil
	})
	return m, home
}

func keysFile(home string) string {
	return filepath.Join(home, ".ssh", "authorized_keys")
}

func writeKeys(t *testing.T, home, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keysFile(home), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readKeys(t *testing.T, home string) string {
	t.Helper()
	data, err := os.ReadFile(keysFile(home))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func spec(keys ...string) *client.AgentSSHSpec {
	return &client.AgentSSHSpec{User: "ubuntu", AuthorizedKeys: keys}
}

func TestReconcileAddsKeysKeepingProviderOnes(t *testing.T) {
	m, home := newManager(t)
	writeKeys(t, home, providerKey+"\n")

	if err := m.Reconcile(spec("ssh-ed25519 AAAAone one@mac", "ssh-rsa AAAAtwo two@win")); err != nil {
		t.Fatal(err)
	}

	got := readKeys(t, home)
	for _, want := range []string{providerKey, "ssh-ed25519 AAAAone one@mac", "ssh-rsa AAAAtwo two@win"} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("authorized_keys lacks %q:\n%s", want, got)
		}
	}
	if !strings.HasPrefix(got, providerKey+"\n") {
		t.Errorf("provider key must stay first:\n%s", got)
	}
}

func TestReconcileReplacesItsOwnKeysOnly(t *testing.T) {
	m, home := newManager(t)
	writeKeys(t, home, providerKey+"\n")
	if err := m.Reconcile(spec("ssh-ed25519 AAAAold old@mac")); err != nil {
		t.Fatal(err)
	}

	if err := m.Reconcile(spec("ssh-ed25519 AAAAnew new@mac")); err != nil {
		t.Fatal(err)
	}

	got := readKeys(t, home)
	if strings.Contains(got, "AAAAold") {
		t.Errorf("key removed from spec must disappear:\n%s", got)
	}
	if !strings.Contains(got, "ssh-ed25519 AAAAnew new@mac\n") || !strings.Contains(got, providerKey+"\n") {
		t.Errorf("unexpected authorized_keys:\n%s", got)
	}
}

func TestReconcileRemovesItsKeysWhenSpecIsEmpty(t *testing.T) {
	m, home := newManager(t)
	writeKeys(t, home, providerKey+"\n")
	if err := m.Reconcile(spec("ssh-ed25519 AAAAone one@mac")); err != nil {
		t.Fatal(err)
	}

	if err := m.Reconcile(spec()); err != nil {
		t.Fatal(err)
	}

	if got := readKeys(t, home); got != providerKey+"\n" {
		t.Errorf("only provider key must remain, got:\n%s", got)
	}
}

func TestReconcileCreatesSSHDirWithStrictModes(t *testing.T) {
	m, home := newManager(t)

	if err := m.Reconcile(spec("ssh-ed25519 AAAAone one@mac")); err != nil {
		t.Fatal(err)
	}

	dir, err := os.Stat(filepath.Join(home, ".ssh"))
	if err != nil {
		t.Fatal(err)
	}
	if dir.Mode().Perm() != 0o700 {
		t.Errorf(".ssh mode = %o, want 700", dir.Mode().Perm())
	}
	file, err := os.Stat(keysFile(home))
	if err != nil {
		t.Fatal(err)
	}
	if file.Mode().Perm() != 0o600 {
		t.Errorf("authorized_keys mode = %o, want 600", file.Mode().Perm())
	}
}

func TestReconcileDropsKeysThatCouldInjectLines(t *testing.T) {
	m, home := newManager(t)

	if err := m.Reconcile(spec("ssh-ed25519 AAAAone one@mac\ncommand=\"rm -rf /\" ssh-rsa AAAAevil", "ssh-rsa AAAAtwo two@win")); err != nil {
		t.Fatal(err)
	}

	got := readKeys(t, home)
	if strings.Contains(got, "AAAAevil") || strings.Contains(got, "AAAAone") {
		t.Errorf("multi-line key must be dropped whole:\n%s", got)
	}
	if !strings.Contains(got, "ssh-rsa AAAAtwo two@win\n") {
		t.Errorf("valid key must stay:\n%s", got)
	}
}

func TestReconcileDoesNotRewriteUnchangedFile(t *testing.T) {
	m, home := newManager(t)
	if err := m.Reconcile(spec("ssh-ed25519 AAAAone one@mac")); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(keysFile(home), past, past); err != nil {
		t.Fatal(err)
	}

	if err := m.Reconcile(spec("ssh-ed25519 AAAAone one@mac")); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(keysFile(home))
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(past) {
		t.Error("unchanged keys must not rewrite authorized_keys")
	}
}

func TestReconcileIgnoresMissingSpec(t *testing.T) {
	m, home := newManager(t)

	if err := m.Reconcile(nil); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(keysFile(home)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("nil spec must not touch the file, stat err = %v", err)
	}
}

func TestReconcileFailsForUnknownUser(t *testing.T) {
	m, _ := newManager(t)

	if err := m.Reconcile(&client.AgentSSHSpec{User: "nobody-here", AuthorizedKeys: []string{"ssh-rsa AAAA x"}}); err == nil {
		t.Error("unknown user must fail")
	}
}

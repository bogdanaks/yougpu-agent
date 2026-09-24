package state

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

var testInclude = []string{"custom_nodes", ".venv", "user", "comfyui.db", "input", ".yougpu"}
var testExclude = []string{"user/__manager/cache", "user/*.log"}

func writeFile(t *testing.T, root, rel, body string, mode os.FileMode) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func exists(root, rel string) bool {
	_, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel)))
	return err == nil
}

func roundTrip(t *testing.T, src string) string {
	t.Helper()
	var buf bytes.Buffer
	if err := Pack(src, testInclude, testExclude, &buf); err != nil {
		t.Fatalf("pack: %v", err)
	}
	dst := t.TempDir()
	if err := Unpack(&buf, dst, 1<<30); err != nil {
		t.Fatalf("unpack: %v", err)
	}
	return dst
}

func TestPackCarriesWorkspaceState(t *testing.T) {
	src := t.TempDir()
	writeFile(t, src, "custom_nodes/pack/__init__.py", "nodes", 0o644)
	writeFile(t, src, ".venv/bin/tool", "#!/bin/sh", 0o755)
	if err := os.Symlink("/usr/bin/python3", filepath.Join(src, ".venv/bin/python")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, "user/default/workflows/w.json", "{}", 0o644)
	writeFile(t, src, "comfyui.db", "db", 0o644)
	stamp := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(src, "custom_nodes/pack/__init__.py"), stamp, stamp); err != nil {
		t.Fatal(err)
	}

	dst := roundTrip(t, src)

	if readFile(t, dst, "custom_nodes/pack/__init__.py") != "nodes" || readFile(t, dst, "comfyui.db") != "db" {
		t.Fatal("content lost")
	}
	if target, err := os.Readlink(filepath.Join(dst, ".venv/bin/python")); err != nil || target != "/usr/bin/python3" {
		t.Fatalf("symlink: %q %v", target, err)
	}
	if info, err := os.Stat(filepath.Join(dst, ".venv/bin/tool")); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("mode: %v %v", info.Mode(), err)
	}
	if info, err := os.Stat(filepath.Join(dst, "custom_nodes/pack/__init__.py")); err != nil || !info.ModTime().Equal(stamp) {
		t.Fatalf("mtime: %v %v", info.ModTime(), err)
	}
}

func TestPackSkipsExcludedAndNotIncluded(t *testing.T) {
	src := t.TempDir()
	writeFile(t, src, "user/__manager/cache/registry.json", "big", 0o644)
	writeFile(t, src, "user/__manager/config.ini", "[default]", 0o644)
	writeFile(t, src, "user/comfyui_8188.log", "log", 0o644)
	writeFile(t, src, "models/checkpoints/m.safetensors", "weights", 0o644)
	writeFile(t, src, "output/out.png", "png", 0o644)

	dst := roundTrip(t, src)

	if !exists(dst, "user/__manager/config.ini") {
		t.Fatal("config lost")
	}
	for _, rel := range []string{"user/__manager/cache", "user/comfyui_8188.log", "models", "output"} {
		if exists(dst, rel) {
			t.Errorf("%s packed", rel)
		}
	}
}

func hostile(t *testing.T, entries ...*tar.Header) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(enc)
	for _, h := range entries {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(strings.Repeat("x", int(h.Size)))); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func regular(name string, size int64) *tar.Header {
	return &tar.Header{Typeflag: tar.TypeReg, Name: name, Size: size, Mode: 0o644}
}

func TestUnpackRejectsPathsOutsideRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "ws")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../escaped", "/etc/escaped", "user/../../escaped"} {
		if err := Unpack(hostile(t, regular(name, 1)), root, 1<<20); err == nil {
			t.Errorf("%s unpacked", name)
		}
	}
	if exists(parent, "escaped") {
		t.Fatal("escaped the workspace")
	}
}

func TestUnpackDoesNotFollowSymlinks(t *testing.T) {
	outside := t.TempDir()
	root := t.TempDir()
	archive := hostile(t, &tar.Header{Typeflag: tar.TypeSymlink, Name: "user", Linkname: outside}, regular("user/planted", 1))
	if err := Unpack(archive, root, 1<<20); err == nil {
		t.Error("wrote through a symlink")
	}
	if exists(outside, "planted") {
		t.Fatal("file planted outside")
	}

	writeFile(t, outside, "victim", "original", 0o644)
	if err := os.Symlink(filepath.Join(outside, "victim"), filepath.Join(root, "comfyui.db")); err != nil {
		t.Fatal(err)
	}
	if err := Unpack(hostile(t, regular("comfyui.db", 3)), root, 1<<20); err != nil {
		t.Fatal(err)
	}
	if readFile(t, outside, "victim") != "original" {
		t.Fatal("followed an existing symlink")
	}
}

func TestUnpackStopsAtLimitAndSkipsSpecialFiles(t *testing.T) {
	if err := Unpack(hostile(t, regular("a", 10), regular("b", 10)), t.TempDir(), 15); err == nil {
		t.Error("limit not enforced")
	}
	root := t.TempDir()
	archive := hostile(t,
		&tar.Header{Typeflag: tar.TypeChar, Name: "dev", Mode: 0o644},
		&tar.Header{Typeflag: tar.TypeLink, Name: "hard", Linkname: "/etc/passwd"},
		regular("ok", 2),
	)
	if err := Unpack(archive, root, 1<<20); err != nil {
		t.Fatal(err)
	}
	if exists(root, "dev") || exists(root, "hard") || !exists(root, "ok") {
		t.Fatal("special files handled wrong")
	}
}

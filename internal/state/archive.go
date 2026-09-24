package state

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

func excluded(patterns []string, rel string) bool {
	for _, pattern := range patterns {
		if ok, _ := path.Match(pattern, rel); ok {
			return true
		}
	}
	return false
}
func Pack(root string, include, exclude []string, w io.Writer) error {
	enc, err := zstd.NewWriter(w)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(enc)

	for _, dir := range include {
		start := filepath.Join(root, filepath.FromSlash(dir))
		if _, err := os.Lstat(start); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if excluded(exclude, rel) {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			return writeEntry(tw, p, rel, d)
		})
		if err != nil {
			return fmt.Errorf("pack %s: %w", dir, err)
		}
	}

	if err := tw.Close(); err != nil {
		return err
	}
	return enc.Close()
}

func writeEntry(tw *tar.Writer, p, rel string, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return err
	}
	h := &tar.Header{Name: rel, Mode: int64(info.Mode().Perm()), ModTime: info.ModTime()}
	switch {
	case d.IsDir():
		h.Typeflag = tar.TypeDir
		h.Name += "/"
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(p)
		if err != nil {
			return err
		}
		h.Typeflag, h.Linkname = tar.TypeSymlink, target
	case info.Mode().IsRegular():
		h.Typeflag, h.Size = tar.TypeReg, info.Size()
	default:
		return nil
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	if h.Typeflag != tar.TypeReg {
		return nil
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.CopyN(tw, f, h.Size)
	return err
}
func Unpack(r io.Reader, root string, maxBytes int64) error {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer dec.Close()
	tr := tar.NewReader(dec)

	var written int64
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := safeRel(h.Name)
		if err != nil {
			return err
		}
		if err := mkdirInside(root, path.Dir(rel)); err != nil {
			return err
		}
		target := filepath.Join(root, filepath.FromSlash(rel))

		switch h.Typeflag {
		case tar.TypeDir:
			if err := mkdirInside(root, rel); err != nil {
				return err
			}
		case tar.TypeReg:
			if written += h.Size; written > maxBytes {
				return fmt.Errorf("state is larger than %d bytes", maxBytes)
			}
			if err := writeRegular(target, tr, h); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := replace(target); err != nil {
				return err
			}
			if err := os.Symlink(h.Linkname, target); err != nil {
				return err
			}
		}
	}
}

func safeRel(name string) (string, error) {
	clean := path.Clean(strings.TrimSuffix(name, "/"))
	if path.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("unsafe path in state: %q", name)
	}
	return clean, nil
}
func mkdirInside(root, rel string) error {
	if rel == "." {
		return nil
	}
	current := root
	for _, part := range strings.Split(rel, "/") {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			if err := os.Mkdir(current, 0o755); err != nil {
				return err
			}
		case err != nil:
			return err
		case !info.IsDir():
			return fmt.Errorf("not a directory in state path: %s", current)
		}
	}
	return nil
}
func replace(target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("directory in place of file: %s", target)
	}
	return os.Remove(target)
}

func writeRegular(target string, r io.Reader, h *tar.Header) error {
	if err := replace(target); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fs.FileMode(h.Mode).Perm())
	if err != nil {
		return err
	}
	if _, err := io.CopyN(f, r, h.Size); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Chtimes(target, h.ModTime, h.ModTime)
}

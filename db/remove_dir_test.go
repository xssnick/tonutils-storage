package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecursiveEmptyDeleteWideDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bag")
	for i := 0; i < 150; i++ {
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("dir-%03d", i)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- recursiveEmptyDelete(buildTreeFromDir(root), NewFSControllerCache(false))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("directory cleanup did not finish")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("directory remains after cleanup: %v", err)
	}
}

func TestRecursiveEmptyDeleteRemovesIgnoredSystemFiles(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bag")
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("failed to create nested dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "desktop.ini"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("failed to create ignored system file: %v", err)
	}

	if err := recursiveEmptyDelete(buildTreeFromDir(root), NewFSControllerCache(false)); err != nil {
		t.Fatalf("expected ignored system files to be removed, got %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("expected root dir to be removed, stat err: %v", err)
	}
}

func TestRecursiveEmptyDeleteReturnsErrorForUnknownFiles(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bag")
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("failed to create nested dir: %v", err)
	}

	keepFile := filepath.Join(nested, "keep.txt")
	if err := os.WriteFile(keepFile, []byte("keep"), 0o644); err != nil {
		t.Fatalf("failed to create keep file: %v", err)
	}

	err := recursiveEmptyDelete(buildTreeFromDir(root), NewFSControllerCache(false))
	if err == nil {
		t.Fatal("expected recursiveEmptyDelete to fail on unknown files")
	}
	if _, statErr := os.Stat(keepFile); statErr != nil {
		t.Fatalf("expected keep file to stay on disk, stat err: %v", statErr)
	}
}

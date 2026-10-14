package db

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/xssnick/tonutils-storage/storage"
)

func TestCreateTorrentFromHiddenDirectoryUsesSourceFiles(t *testing.T) {
	st := memoryStorage(t)
	parent := t.TempDir()
	source := filepath.Join(parent, ".hidden")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(source, "payload.bin")
	sibling := filepath.Join(parent, "payload.bin")
	if err := os.WriteFile(file, []byte("source content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sibling, []byte("unrelated sibling"), 0o600); err != nil {
		t.Fatal(err)
	}

	tr := createLocalTorrent(t, st, source, nil)
	report, err := tr.CheckPiecesProofs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if report.OKPieces != report.TotalPieces {
		t.Fatalf("source pieces did not verify: %+v", report)
	}
	if err := st.RemoveTorrent(tr, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("source content remains after removal: %v", err)
	}
	if data, err := os.ReadFile(sibling); err != nil || string(data) != "unrelated sibling" {
		t.Fatalf("sibling content changed: %q, %v", data, err)
	}
}

func TestGetAllFilesRefsInDirExcludesNestedSystemFiles(t *testing.T) {
	st := memoryStorage(t)
	root := t.TempDir()
	for _, dir := range []string{root, filepath.Join(root, "nested", "deeper")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{".DS_Store", "desktop.ini", "Thumbs.db"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("system metadata"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	want := []string{"nested/deeper/content.bin", "nested/notes.DS_Store"}
	for _, name := range want {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte("content"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := st.GetAllFilesRefsInDir(root)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.GetName())
	}
	sort.Strings(names)
	if !slices.Equal(names, want) {
		t.Fatalf("got bag files %v, want %v", names, want)
	}
}

func TestCreateTorrentFromPartialDownloadExcludesGeneratedCache(t *testing.T) {
	for _, cacheDir := range []string{".tonutils-storage-pieces", ".TONUTILS-STORAGE-PIECES"} {
		t.Run(cacheDir, func(t *testing.T) {
			st := memoryStorage(t)
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "selected.bin"), []byte("selected file data"), 0o600); err != nil {
				t.Fatal(err)
			}
			cache := filepath.Join(root, cacheDir, "bag", "1")
			if err := os.MkdirAll(filepath.Dir(cache), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(cache, []byte("unselected boundary bytes"), 0o600); err != nil {
				t.Fatal(err)
			}
			refs, err := st.GetAllFilesRefsInDir(root)
			if err != nil {
				t.Fatal(err)
			}
			tr, err := storage.CreateTorrentWithInitialHeader(context.Background(), root, "", &storage.TorrentHeader{}, st, nil, refs, nil, false)
			if err != nil {
				t.Fatalf("failed to create bag after partial download: %v", err)
			}
			files, err := tr.ListFiles()
			if err != nil || len(files) != 1 || files[0] != "selected.bin" {
				t.Fatalf("generated cache became bag content: %v, %v", files, err)
			}
		})
	}
}

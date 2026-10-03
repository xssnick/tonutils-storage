package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xssnick/tonutils-storage/storage"
)

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

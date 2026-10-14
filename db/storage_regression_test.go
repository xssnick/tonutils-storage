package db

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xssnick/tonutils-storage/storage"
)

func TestRemoveDownloadedTorrentCleansOwnedDirectories(t *testing.T) {
	st := memoryStorage(t)
	root := filepath.Join(t.TempDir(), ".download")
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "content.bin"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	tr := createLocalTorrent(t, st, root, nil)
	tr.CreatedLocally = false
	if err := st.SetTorrent(tr); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, ".DS_Store"), []byte("metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(root, ".tonutils-storage-pieces", hex.EncodeToString(tr.BagID))
	if err := os.MkdirAll(cache, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache, "0"), []byte("cached piece"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveTorrent(tr, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("download directory remains: %v", err)
	}
}

func createLocalTorrent(t *testing.T, st *Storage, source string, only map[string]bool) *storage.Torrent {
	t.Helper()
	root, dir, refs, err := st.DetectFileRefs(source, only)
	if err != nil {
		t.Fatal(err)
	}
	header := &storage.TorrentHeader{DirName: []byte(dir), DirNameSize: uint32(len(dir))}
	tr, err := storage.CreateTorrentWithInitialHeader(t.Context(), root, "", header, st, nil, refs, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetTorrent(tr); err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestRemoveLocalTorrentPreservesUnrelatedContent(t *testing.T) {
	for _, mode := range []string{"single file", "selected directory files"} {
		t.Run(mode, func(t *testing.T) {
			st := memoryStorage(t)
			root := t.TempDir()
			selected := filepath.Join(root, "selected.bin")
			if mode == "selected directory files" {
				selected = filepath.Join(root, "owned", "selected.bin")
				if err := os.Mkdir(filepath.Dir(selected), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			unrelated := filepath.Join(root, "keep.bin")
			emptyDir := filepath.Join(root, "unrelated-empty")
			if err := os.Mkdir(emptyDir, 0o700); err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{selected, unrelated} {
				if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			source := selected
			if mode == "selected directory files" {
				source = root
			}
			tr := createLocalTorrent(t, st, source, map[string]bool{selected: true})
			if err := st.RemoveTorrent(tr, true); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(selected); !os.IsNotExist(err) {
				t.Fatalf("selected file remains: %v", err)
			}
			if data, err := os.ReadFile(unrelated); err != nil || string(data) != "content" {
				t.Fatalf("unrelated file changed: %q, %v", data, err)
			}
			if _, err := os.Stat(emptyDir); err != nil {
				t.Fatalf("unrelated empty directory was removed: %v", err)
			}
			if mode == "selected directory files" {
				if _, err := os.Stat(filepath.Dir(selected)); !os.IsNotExist(err) {
					t.Fatalf("empty bag directory remains: %v", err)
				}
			}
			if st.GetTorrent(tr.BagID) != nil {
				t.Fatal("removed bag remains in memory")
			}
			if _, err := st.db.Get(append([]byte("bags:"), tr.BagID...), nil); !errors.Is(err, leveldb.ErrNotFound) {
				t.Fatalf("bag metadata remains: %v", err)
			}
		})
	}
}

type removalFailureController struct {
	storage.FSController
	path string
	err  error
}

func (f removalFailureController) RemoveFile(path string) error {
	if filepath.Clean(path) == f.path {
		return f.err
	}
	return f.FSController.RemoveFile(path)
}

func TestRemoveLocalTorrentReportsFilesystemFailures(t *testing.T) {
	for _, failOn := range []string{"file", "directory"} {
		t.Run(failOn, func(t *testing.T) {
			st := memoryStorage(t)
			root := filepath.Join(t.TempDir(), "source")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(root, "content.bin")
			if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
				t.Fatal(err)
			}
			tr := createLocalTorrent(t, st, root, nil)
			failedPath := file
			if failOn == "directory" {
				failedPath = root
			}
			failure := errors.New("filesystem removal failed")
			st.fs.ctrl = removalFailureController{FSController: st.fs.ctrl, path: failedPath, err: failure}
			if err := st.RemoveTorrent(tr, true); !errors.Is(err, failure) {
				t.Fatalf("filesystem failure was not reported: %v", err)
			}
			if st.GetTorrent(tr.BagID) == nil {
				t.Fatal("bag metadata removed despite filesystem failure")
			}
		})
	}
}

func TestRemoveLocalTorrentWithNoRemoveKeepsContent(t *testing.T) {
	st := memoryStorage(t)
	root := t.TempDir()
	file := filepath.Join(root, "content.bin")
	if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	tr := createLocalTorrent(t, st, root, nil)
	st.fs.ctrl = NewFSControllerCache(true)
	if err := st.RemoveTorrent(tr, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("protected content was removed: %v", err)
	}
	if st.GetTorrent(tr.BagID) != nil {
		t.Fatal("removed bag remains in memory")
	}
}

package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestActiveFilesSelectionPreservesLocallyCreatedSource(t *testing.T) {
	for _, test := range []struct {
		name        string
		active      bool
		changed     string
		missing     bool
		selectNames bool
	}{
		{name: "active_appended", active: true, changed: "original + additional important bytes"},
		{name: "active_modified_by_name", active: true, changed: "edited", selectNames: true},
		{name: "active_missing", active: true, missing: true},
		{name: "paused_appended", changed: "original + additional important bytes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			sourcePath := filepath.Join(root, "payload")
			if err := os.WriteFile(sourcePath, []byte("original"), 0o644); err != nil {
				t.Fatal(err)
			}
			store := newE2EStorage(4096)
			torrent, err := CreateTorrentWithInitialHeader(t.Context(), root, "", &TorrentHeader{}, store, nil,
				[]FileRef{e2eFileRef{path: sourcePath, name: "payload", size: 8}}, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if test.missing {
				err = os.Remove(sourcePath)
			} else {
				err = os.WriteFile(sourcePath, []byte(test.changed), 0o644)
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.active {
				torrent.globalCtx, torrent.pause = context.WithCancel(t.Context())
			}
			t.Cleanup(func() {
				torrent.Stop()
				torrent.Wait()
			})
			torrent.downloadAll = true
			if test.selectNames {
				err = torrent.SetActiveFiles([]string{"payload"})
			} else {
				err = torrent.SetActiveFilesIDs([]uint32{0})
			}
			if err != nil {
				t.Fatal(err)
			}
			torrent.Wait()
			got, err := os.ReadFile(sourcePath)
			if test.missing {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("selection recreated a missing local source: data=%q err=%v", got, err)
				}
			} else if err != nil || !bytes.Equal(got, []byte(test.changed)) {
				t.Fatalf("selection changed the local source: data=%q err=%v", got, err)
			}
			active, _ := torrent.IsActive()
			if active != test.active {
				t.Fatalf("selection changed active state: got %v, want %v", active, test.active)
			}
			selected, err := store.GetActiveFiles(torrent.BagID)
			if err != nil || len(selected) != 1 || selected[0] != 0 || torrent.IsDownloadAll() {
				t.Fatalf("selection was not saved: selected=%v downloadAll=%v err=%v", selected, torrent.IsDownloadAll(), err)
			}
		})
	}
}

func TestLoopbackOrderedDownloadAllowsDotsWithinFileNames(t *testing.T) {
	files := map[string][]byte{
		"archive..bak": bytes.Repeat([]byte("archive"), 1024),
		"movie...mp4":  []byte("movie"),
	}
	_, torrent := setupLoopbackFiles(t, files, nil, true)
	completeTestDownload(t, torrent)
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(torrent.Path, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("downloaded file %s mismatch: data length=%d err=%v", name, len(got), err)
		}
	}
	if !torrent.IsCompleted() {
		t.Fatal("ordered download did not commit every piece")
	}
}

func TestWriteOrderedRejectsTraversalComponents(t *testing.T) {
	for _, name := range []string{"../outside", "nested/../outside", "./outside"} {
		t.Run(name, func(t *testing.T) {
			list := []fileInfo{{path: name, info: &FileInfo{}}}
			if err := writeOrdered(t.Context(), &Torrent{}, list, nil, t.TempDir(), 4096, nil, nil); err == nil {
				t.Fatal("ordered downloader accepted traversal component")
			}
		})
	}
}

package db

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	levelstorage "github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/xssnick/tonutils-storage/storage"
)

func memoryStorage(t *testing.T) *Storage {
	t.Helper()
	ldb, err := leveldb.Open(levelstorage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	st, err := NewStorage(ldb, nil, 0, false, false, false, nil)
	if err != nil {
		_ = ldb.Close()
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func TestSetPiecesStoresWholeCommitGroup(t *testing.T) {
	st := memoryStorage(t)
	bagID := bytes.Repeat([]byte{1}, 32)
	pieces := make(map[uint32]*storage.PieceInfo)
	for id := uint32(0); id < 32; id++ {
		pieces[id] = &storage.PieceInfo{StartFileIndex: id + 4, Proof: []byte{byte(id)}}
	}
	if err := st.SetPieces(bagID, pieces); err != nil {
		t.Fatal(err)
	}
	for id, expected := range pieces {
		actual, err := st.GetPiece(bagID, id)
		if err != nil || actual.StartFileIndex != expected.StartFileIndex || !bytes.Equal(actual.Proof, expected.Proof) {
			t.Fatalf("piece %d: %+v, %v", id, actual, err)
		}
	}
	mask, err := st.PiecesMaskWithError(bagID, 32)
	if err != nil || !bytes.Equal(mask, []byte{255, 255, 255, 255}) {
		t.Fatalf("incorrect commit mask: %v, %v", mask, err)
	}
}

func TestSetPiecesRejectsInvalidGroupWithoutPartialWrites(t *testing.T) {
	st := memoryStorage(t)
	bagID := bytes.Repeat([]byte{1}, 32)
	if err := st.SetPieces(bagID, map[uint32]*storage.PieceInfo{
		0: {Proof: []byte("proof")},
		1: nil,
	}); err == nil {
		t.Fatal("invalid commit group was accepted")
	}
	if _, err := st.GetPiece(bagID, 0); !errors.Is(err, leveldb.ErrNotFound) {
		t.Fatalf("partial group was stored: %v", err)
	}
}

func TestPiecesMaskReleasesIteratorsAndReportsErrors(t *testing.T) {
	st := memoryStorage(t)
	bagID := bytes.Repeat([]byte{1}, 32)
	if err := st.SetPiece(bagID, 0, &storage.PieceInfo{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := st.PiecesMaskWithError(bagID, 1); err != nil {
			t.Fatal(err)
		}
	}
	alive, err := st.db.GetProperty("leveldb.aliveiters")
	if err != nil || alive != "0" {
		t.Fatalf("iterators remain alive: %q, %v", alive, err)
	}
	if err := st.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PiecesMaskWithError(bagID, 1); !errors.Is(err, leveldb.ErrClosed) {
		t.Fatalf("iterator failure was not reported: %v", err)
	}
}

func TestRemoveTorrentCleansAllMetadataAndGeneratedCache(t *testing.T) {
	st := memoryStorage(t)
	root := t.TempDir()
	bagID := bytes.Repeat([]byte{1}, 32)
	otherID := bytes.Repeat([]byte{2}, 32)
	tr := storage.NewTorrent(root, st, nil)
	tr.BagID = bagID
	// Removal must also work before torrent metadata was loaded.
	if err := st.SetTorrent(tr); err != nil {
		t.Fatal(err)
	}
	if err := st.SetActiveFiles(bagID, []uint32{1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPiece(bagID, 999, &storage.PieceInfo{}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPiece(otherID, 0, &storage.PieceInfo{}); err != nil {
		t.Fatal(err)
	}
	content := filepath.Join(root, "user-file")
	if err := os.WriteFile(content, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(root, ".tonutils-storage-pieces", hex.EncodeToString(bagID))
	if err := os.MkdirAll(cache, 0o700); err != nil {
		t.Fatal(err)
	}
	cacheFile := filepath.Join(cache, "999")
	if err := os.WriteFile(cacheFile, []byte("piece"), 0o600); err != nil {
		t.Fatal(err)
	}
	st.fs.ctrl = NewFSControllerCache(true)
	if _, err := st.fs.ctrl.AcquireRead(cacheFile, make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveTorrent(tr, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cache); !os.IsNotExist(err) {
		t.Fatalf("generated cache remains: %v", err)
	}
	if _, err := os.Stat(content); err != nil {
		t.Fatalf("user content was removed: %v", err)
	}
	if _, err := st.GetActiveFiles(bagID); !errors.Is(err, leveldb.ErrNotFound) {
		t.Fatalf("active files remain: %v", err)
	}
	if _, err := st.GetPiece(bagID, 999); !errors.Is(err, leveldb.ErrNotFound) {
		t.Fatalf("piece metadata remains: %v", err)
	}
	if _, err := st.GetPiece(otherID, 0); err != nil {
		t.Fatalf("unrelated bag was modified: %v", err)
	}
	if st.GetTorrent(bagID) != nil {
		t.Fatal("removed torrent remains in memory")
	}
}

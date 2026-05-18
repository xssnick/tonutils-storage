package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type failingStorage struct {
	setPieceErr    error
	removePieceErr error
}

func (f *failingStorage) GetFS() FS { return nil }

func (f *failingStorage) GetAll() []*Torrent { return nil }

func (f *failingStorage) GetTorrentByOverlay([]byte) *Torrent { return nil }

func (f *failingStorage) SetTorrent(*Torrent) error { return nil }

func (f *failingStorage) SetActiveFiles([]byte, []uint32) error { return nil }

func (f *failingStorage) GetActiveFiles([]byte) ([]uint32, error) { return nil, nil }

func (f *failingStorage) GetPiece([]byte, uint32) (*PieceInfo, error) { return nil, ErrFileNotExist }

func (f *failingStorage) RemovePiece([]byte, uint32) error { return f.removePieceErr }

func (f *failingStorage) SetPiece([]byte, uint32, *PieceInfo) error { return f.setPieceErr }

func (f *failingStorage) PiecesMask([]byte, uint32) []byte { return nil }

func (f *failingStorage) UpdateUploadStats([]byte, uint64) error { return nil }

func (f *failingStorage) VerifyOnStartup() bool { return false }

func (f *failingStorage) GetForcedPieceSize() uint32 { return 0 }

type recordingStorage struct {
	failingStorage
	setPieceCalls      int
	setPieceBeforeSync bool
	uploadStats        []uint64
	syncFile           *syncTrackingFile
}

func (r *recordingStorage) SetPiece(bagID []byte, id uint32, p *PieceInfo) error {
	r.setPieceCalls++
	if r.syncFile != nil && !r.syncFile.synced {
		r.setPieceBeforeSync = true
	}
	return nil
}

func (r *recordingStorage) UpdateUploadStats(_ []byte, val uint64) error {
	r.uploadStats = append(r.uploadStats, val)
	return nil
}

type syncTrackingFile struct {
	synced bool
	syncs  int
}

func (s *syncTrackingFile) ReadAt([]byte, int64) (int, error) {
	return 0, nil
}

func (s *syncTrackingFile) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

func (s *syncTrackingFile) Close() error {
	return nil
}

func (s *syncTrackingFile) Sync() error {
	s.synced = true
	s.syncs++
	return nil
}

func TestTorrent_IsCompleted(t *testing.T) {
	tr := Torrent{
		pieceMask: []byte{0xff, 0xff, 0xC0}, // 18
	}

	for i := 18; i < 24; i++ {
		tr.Info = &TorrentInfo{
			FileSize:  uint64(i),
			PieceSize: 1,
		}

		if i == 18 {
			if !tr.IsCompleted() {
				t.Fatal("should be completed", i)
			}
		} else {
			if tr.IsCompleted() {
				t.Fatal("should be not completed", i)
			}
		}
	}
}

func TestTorrentDownloadedPiecesCounterTracksSetAndRemove(t *testing.T) {
	tr := &Torrent{
		BagID:     make([]byte, 32),
		db:        &recordingStorage{},
		Info:      &TorrentInfo{PieceSize: 1, FileSize: 1},
		pieceMask: []byte{0},
	}

	if err := tr.setPiece(0, &PieceInfo{StartFileIndex: 0, Proof: []byte{1}}, false); err != nil {
		t.Fatalf("failed to set piece: %v", err)
	}
	if got := tr.DownloadedPiecesNum(); got != 1 {
		t.Fatalf("expected one downloaded piece, got %d", got)
	}
	if !tr.IsCompleted() {
		t.Fatal("expected single-piece torrent to be completed")
	}

	if err := tr.removePiece(0); err != nil {
		t.Fatalf("failed to remove piece: %v", err)
	}
	if got := tr.DownloadedPiecesNum(); got != 0 {
		t.Fatalf("expected no downloaded pieces after remove, got %d", got)
	}
}

func TestTorrent_GetFilesInPiece_NoOverlapForAdjacentFiles(t *testing.T) {
	tr := &Torrent{
		Info: &TorrentInfo{
			PieceSize:  1024,
			HeaderSize: 0,
			FileSize:   2048,
		},
		Header: &TorrentHeader{
			FilesCount:    2,
			TotalNameSize: 2,
			DataIndex:     []uint64{1024, 2048},
			NameIndex:     []uint64{1, 2},
			Names:         []byte("ab"),
		},
	}

	files, err := tr.GetFilesInPiece(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("expected a single file in piece, got %d", len(files))
	}

	if files[0].Index != 1 {
		t.Fatalf("expected file with index 1, got %d", files[0].Index)
	}

	first, err := tr.GetFileOffsetsByID(0)
	if err != nil {
		t.Fatalf("unexpected first file offset error: %v", err)
	}
	if first.FromPiece != 0 || first.ToPiece != 0 || first.ToPieceOffset != 1024 {
		t.Fatalf("expected first file to end in piece 0 at offset 1024, got from=%d to=%d offset=%d", first.FromPiece, first.ToPiece, first.ToPieceOffset)
	}

	second, err := tr.GetFileOffsetsByID(1)
	if err != nil {
		t.Fatalf("unexpected second file offset error: %v", err)
	}
	if second.FromPiece != 1 || second.ToPiece != 1 || second.ToPieceOffset != 1024 {
		t.Fatalf("expected second file to end in piece 1 at offset 1024, got from=%d to=%d offset=%d", second.FromPiece, second.ToPiece, second.ToPieceOffset)
	}
}

func TestTorrent_GetFilesInPieceFindsOverlapsWithHeaderOffset(t *testing.T) {
	tr := &Torrent{
		Info: &TorrentInfo{
			PieceSize:  1024,
			HeaderSize: 1024,
			FileSize:   4096,
		},
		Header: &TorrentHeader{
			FilesCount:    3,
			TotalNameSize: 3,
			DataIndex:     []uint64{512, 2048, 3072},
			NameIndex:     []uint64{1, 2, 3},
			Names:         []byte("abc"),
		},
	}

	files, err := tr.GetFilesInPiece(0)
	if err != nil {
		t.Fatalf("unexpected header-only piece error: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected header-only piece to contain no files, got %d", len(files))
	}

	files, err = tr.GetFilesInPiece(2)
	if err != nil {
		t.Fatalf("unexpected data piece error: %v", err)
	}
	if len(files) != 1 || files[0].Index != 1 {
		t.Fatalf("expected piece 2 to contain only file 1, got %#v", files)
	}
}

func TestPieceCommitterSyncsBeforePersistingPiece(t *testing.T) {
	file := &syncTrackingFile{}
	store := &recordingStorage{syncFile: file}
	tr := &Torrent{
		BagID:     make([]byte, 32),
		db:        store,
		pieceMask: []byte{0},
	}
	committer := newPieceCommitter(tr)

	committer.MarkDirty(file)
	if err := committer.Add(0, 0, []byte{1, 2, 3}); err != nil {
		t.Fatalf("unexpected add error: %v", err)
	}
	if store.setPieceCalls != 0 {
		t.Fatalf("piece persisted before explicit flush: %d calls", store.setPieceCalls)
	}
	if err := committer.Flush(); err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if store.setPieceBeforeSync {
		t.Fatal("piece was persisted before file sync")
	}
	if file.syncs != 1 {
		t.Fatalf("expected one file sync, got %d", file.syncs)
	}
	if store.setPieceCalls != 1 {
		t.Fatalf("expected one persisted piece, got %d", store.setPieceCalls)
	}
}

func TestTorrentStartDownloadHeaderOnlyCompletesWithoutPieces(t *testing.T) {
	tr := &Torrent{
		BagID:     make([]byte, 32),
		db:        &failingStorage{},
		globalCtx: context.Background(),
		wake:      newWakeSig(),
		Info: &TorrentInfo{
			PieceSize:  4096,
			FileSize:   8192,
			HeaderSize: 4096,
		},
		Header: &TorrentHeader{
			FilesCount:    1,
			TotalNameSize: uint64(len("file.bin")),
			NameIndex:     []uint64{uint64(len("file.bin"))},
			DataIndex:     []uint64{4096},
			Names:         []byte("file.bin"),
		},
	}

	done := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	if err := tr.startDownload(func(event Event) {
		switch event.Name {
		case EventDone:
			done <- struct{}{}
		case EventErr:
			errCh <- event.Value.(error)
		}
	}); err != nil {
		t.Fatalf("unexpected startDownload error: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("header-only download emitted error: %v", err)
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for header-only download to complete")
	}
}

func TestTorrentHeaderParseRejectsTooManyFilesBeforeIndexing(t *testing.T) {
	var buf bytes.Buffer
	header := TorrentHeader{
		FilesCount: maxTorrentHeaderFiles + 1,
	}
	if err := header.Serialize(&buf); err != nil {
		t.Fatalf("failed to serialize test header: %v", err)
	}

	var parsed TorrentHeader
	if _, err := parsed.Parse(buf.Bytes()); err == nil {
		t.Fatal("expected oversized header to be rejected")
	}
}

func TestTorrentHeaderParseRejectsMalformedIndexes(t *testing.T) {
	var buf bytes.Buffer
	header := TorrentHeader{
		FilesCount:    2,
		TotalNameSize: 2,
		NameIndex:     []uint64{2, 1},
		DataIndex:     []uint64{1, 2},
		Names:         []byte("ab"),
	}
	if err := header.Serialize(&buf); err != nil {
		t.Fatalf("failed to serialize test header: %v", err)
	}

	var parsed TorrentHeader
	if _, err := parsed.Parse(buf.Bytes()); err == nil {
		t.Fatal("expected non-monotonic header indexes to be rejected")
	}
}

func TestTorrentSetPieceKeepsMaskUnchangedOnStorageError(t *testing.T) {
	tr := &Torrent{
		BagID:     make([]byte, 32),
		db:        &failingStorage{setPieceErr: errors.New("disk full")},
		pieceMask: []byte{0},
	}

	err := tr.setPiece(3, &PieceInfo{StartFileIndex: 0, Proof: []byte{1, 2, 3}}, false)
	if err == nil {
		t.Fatal("expected setPiece to fail")
	}
	if tr.pieceMask[0] != 0 {
		t.Fatalf("piece mask changed after failed setPiece: %08b", tr.pieceMask[0])
	}
	if len(tr.newPieces) != 0 {
		t.Fatalf("newPieces should stay empty on failed setPiece, got %v", tr.newPieces)
	}
}

func TestTorrentRemovePieceKeepsMaskUnchangedOnStorageError(t *testing.T) {
	tr := &Torrent{
		BagID:     make([]byte, 32),
		db:        &failingStorage{removePieceErr: errors.New("disk full")},
		pieceMask: []byte{1 << 3},
	}

	err := tr.removePiece(3)
	if err == nil {
		t.Fatal("expected removePiece to fail")
	}
	if tr.pieceMask[0] != 1<<3 {
		t.Fatalf("piece mask changed after failed removePiece: %08b", tr.pieceMask[0])
	}
}

func TestTorrentReportDownloadEventPausesOnlyCurrentDownload(t *testing.T) {
	currentFlag := new(bool)
	otherFlag := new(bool)
	paused := 0
	reported := 0

	tr := &Torrent{
		BagID:               make([]byte, 32),
		currentDownloadFlag: currentFlag,
		pause: func() {
			paused++
		},
	}

	tr.reportDownloadEvent(currentFlag, func(Event) {
		reported++
	}, Event{Name: EventErr, Value: errors.New("disk full")})
	if paused != 1 {
		t.Fatalf("expected pause for current download error, got %d", paused)
	}
	if reported != 1 {
		t.Fatalf("expected report callback to be invoked once, got %d", reported)
	}

	tr.currentDownloadFlag = otherFlag
	tr.reportDownloadEvent(currentFlag, nil, Event{Name: EventErr, Value: errors.New("disk full")})
	if paused != 1 {
		t.Fatalf("stale download error should not pause current torrent, got %d", paused)
	}

	tr.currentDownloadFlag = currentFlag
	tr.reportDownloadEvent(currentFlag, nil, Event{Name: EventErr, Value: context.Canceled})
	if paused != 1 {
		t.Fatalf("context cancellation should not pause torrent again, got %d", paused)
	}
}

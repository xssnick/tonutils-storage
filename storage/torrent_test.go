package storage

import (
	"testing"
)

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

func TestTorrent_GetFilesInPiece_NoOverlapForAdjacentFiles(t *testing.T) {
	tr := &Torrent{
		Info: &TorrentInfo{
			PieceSize:  1024,
			HeaderSize: 0,
		},
		Header: &TorrentHeader{
			FilesCount: 2,
			DataIndex:  []uint64{1024, 2048},
			NameIndex:  []uint64{1, 2},
			Names:      []byte("ab"),
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
}

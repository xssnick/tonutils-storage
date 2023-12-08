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

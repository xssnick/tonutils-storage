package storage

import (
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
)

func TestDownloadRejectsProofValidPieceWithWrongLength(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(map[bool]string{false: "short", true: "oversized"}[oversized], func(t *testing.T) {
			torrent, err := CreateTorrentWithInitialHeader(t.Context(), t.TempDir(), "", &TorrentHeader{}, newE2EStorage(512), nil,
				[]FileRef{memoryFileRef{name: "empty"}}, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			piece, err := torrent.getPieceInternal(0, true)
			if err != nil {
				t.Fatal(err)
			}
			// The proof is valid for the payload but conflicts with the declared
			// total length. Hash verification alone cannot validate these ranges.
			if oversized {
				torrent.Info.FileSize--
			} else {
				torrent.Info.FileSize++
			}
			peer := &storagePeer{torrent: torrent, conn: &PeerConnection{
				dataQueue: make(chan struct{}, 1),
				rldp: &testRLDP{onQuery: func(_ tl.Serializable, result tl.Serializable) error {
					*result.(*Piece) = *piece
					return nil
				}},
			}}
			if _, _, err = peer.downloadPiece(t.Context(), 0); err == nil || !strings.Contains(err.Error(), "incorrect size") {
				t.Fatalf("expected piece length validation error, got %v", err)
			}
		})
	}
}

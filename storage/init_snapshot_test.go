package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func newInitSnapshotTestPeer(mask []byte) (*storagePeer, *testRLDP) {
	transport := &testRLDP{}
	torrent := &Torrent{
		BagID:     []byte("bag"),
		Info:      &TorrentInfo{PieceSize: 1, FileSize: uint64(len(mask) * 8)},
		pieceMask: mask,
		globalCtx: context.Background(),
		peers:     map[string]*PeerInfo{},
	}
	conn := &PeerConnection{
		adnl:             &testADNLPeer{},
		rldp:             transport,
		usedByBags:       map[string]*storagePeer{},
		initControlQueue: make(chan struct{}, 1),
		controlQueue:     make(chan struct{}, 1),
	}
	sessionID := int64(1)
	peer, _ := torrent.prepareStoragePeer([]byte("overlay"), nil, conn, &sessionID)
	torrent.peers["peer"] = &PeerInfo{peer: peer}
	return peer, transport
}

func TestInitSnapshotPreservesNewPiecesWhileSending(t *testing.T) {
	peer, transport := newInitSnapshotTestPeer(make([]byte, maxPiecesBytesPerRequest+1))
	torrent := peer.torrent
	torrent.newPieces = []uint32{0}
	lastPiece := torrent.Info.PiecesNum() - 1
	first := true
	transport.onQuery = func(query, _ tl.Serializable) error {
		if !first {
			return nil
		}
		first = false
		torrent.maskMx.Lock()
		torrent.pieceMask[len(torrent.pieceMask)-1] = 0x80
		torrent.maskMx.Unlock()
		torrent.enqueueNewPiece(lastPiece)
		torrent.compactNewPieces()
		return nil
	}
	if err := peer.updateInitPieces(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(transport.queries) != 2 {
		t.Fatalf("got %d init chunks, want 2", len(transport.queries))
	}
	tail := transport.queries[1].Update.(UpdateInit)
	if !bytes.Equal(tail.HavePieces, []byte{0}) {
		t.Fatalf("init snapshot changed during transmission: %x", tail.HavePieces)
	}
	have, _ := torrent.getNewPiecesSince(1, 0)
	if len(have) != 1 || have[0] != int32(lastPiece) {
		t.Fatalf("piece added during init was compacted: %v", have)
	}
	if err := peer.updateHavePieces(t.Context()); err != nil {
		t.Fatal(err)
	}
	update := transport.queries[2].Update.(UpdateHavePieces)
	if len(update.PieceIDs) != 1 || update.PieceIDs[0] != int32(lastPiece) {
		t.Fatalf("piece added during init was not announced: %v", update.PieceIDs)
	}
	if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 0 {
		t.Fatalf("announced pieces remain in journal: %v", have)
	}
}

func TestInitSnapshotRetryUsesFreshMask(t *testing.T) {
	peer, transport := newInitSnapshotTestPeer([]byte{0})
	torrent := peer.torrent
	expected := errors.New("init failed")
	transport.onQuery = func(query, _ tl.Serializable) error {
		torrent.maskMx.Lock()
		torrent.pieceMask[0] = 0x08
		torrent.maskMx.Unlock()
		torrent.enqueueNewPiece(3)
		torrent.compactNewPieces()
		return expected
	}
	if err := peer.updateInitPieces(t.Context()); !errors.Is(err, expected) {
		t.Fatalf("got %v, want init failure", err)
	}
	if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 1 || have[0] != 3 {
		t.Fatalf("failed init discarded unannounced piece: %v", have)
	}
	transport.onQuery = nil
	if err := peer.updateInitPieces(t.Context()); err != nil {
		t.Fatal(err)
	}
	update := transport.queries[0].Update.(UpdateInit)
	if !bytes.Equal(update.HavePieces, []byte{0x08}) {
		t.Fatalf("retry reused stale snapshot: %x", update.HavePieces)
	}
	if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 0 {
		t.Fatalf("piece included in fresh snapshot remains in journal: %v", have)
	}
}

func TestInitSnapshotReinitRejectsStaleSender(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peer, transport := newInitSnapshotTestPeer(make([]byte, maxPiecesBytesPerRequest+1))
		torrent := peer.torrent
		attempt := peer.currentSessionAttempt(t.Context())
		entered, release := make(chan struct{}), make(chan struct{})
		transport.onQuery = func(query, _ tl.Serializable) error {
			request, _ := overlay.UnwrapQuery(query)
			if request.(AddUpdate).SessionID == attempt.id {
				close(entered)
				<-release
			}
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- peer.sendInitPieces(t.Context(), attempt) }()
		<-entered
		torrent.enqueueNewPiece(7)
		torrent.compactNewPieces()
		if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 1 {
			t.Fatal("in-progress init did not preserve its journal")
		}
		newID := attempt.id + 1
		_, next := torrent.prepareStoragePeer(peer.overlay, nil, peer.conn, &newID)
		if next == nil {
			t.Fatal("new session did not create an init attempt")
		}
		torrent.compactNewPieces()
		if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 0 {
			t.Fatal("reinit retained the previous session's init marker")
		}
		close(release)
		if err := <-done; !errors.Is(err, errStaleSessionAttempt) {
			t.Fatalf("got %v, want stale session", err)
		}
		torrent.enqueueNewPiece(6)
		torrent.compactNewPieces()
		if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 0 {
			t.Fatal("stale sender restored the previous session's init marker")
		}
		if err := peer.sendInitPieces(next.ctx, *next); err != nil {
			t.Fatal(err)
		}
		torrent.enqueueNewPiece(5)
		torrent.compactNewPieces()
		if have, _ := torrent.getNewPiecesSince(0, 0); len(have) != 1 || have[0] != 5 {
			t.Fatalf("new session did not preserve its unannounced piece: %v", have)
		}
	})
}

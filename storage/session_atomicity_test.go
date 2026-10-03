package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionReinitWaitsForValidatedUpdate(t *testing.T) {
	tor := &Torrent{BagID: []byte("bag"), Info: &TorrentInfo{PieceSize: 1, FileSize: 8}, globalCtx: context.Background(), peers: map[string]*PeerInfo{}, wake: newWakeSig()}
	conn := &PeerConnection{adnl: &testADNLPeer{}, usedByBags: map[string]*storagePeer{}}
	oldID := int64(1)
	peer, _ := tor.prepareStoragePeer([]byte("overlay"), nil, conn, &oldID)
	peer.piecesMx.Lock()
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- tor.routeIncomingSessionUpdate(peer, peer.nodeId, incomingSessionUpdate{sessionID: oldID, seqno: 1, update: UpdateInit{HavePieces: []byte{0xff}, State: State{WillUpload: true}}})
	}()
	deadline := time.Now().Add(time.Second)
	for peer.sessionMx.TryLock() {
		peer.sessionMx.Unlock()
		if time.Now().After(deadline) {
			peer.piecesMx.Unlock()
			t.Fatal("update did not acquire session lock")
		}
		time.Sleep(time.Millisecond)
	}
	resetStarted, resetDone := make(chan struct{}), make(chan struct{})
	newID := int64(2)
	go func() {
		close(resetStarted)
		tor.prepareStoragePeer([]byte("overlay"), nil, conn, &newID)
		close(resetDone)
	}()
	<-resetStarted
	select {
	case <-resetDone:
		t.Fatal("reset interleaved with validated session update")
	case <-time.After(20 * time.Millisecond):
	}
	peer.piecesMx.Unlock()
	if err := <-updateDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("session reset deadlocked")
	}
	if atomic.LoadInt64(&peer.session.sessionId) != newID || atomic.LoadInt32(&peer.session.remoteInitComplete) != 0 || atomic.LoadUint32(&peer.knownPieces) != 0 {
		t.Fatal("old session update survived reset")
	}
	if err := tor.routeIncomingSessionUpdate(peer, peer.nodeId, incomingSessionUpdate{sessionID: oldID, seqno: 2, update: UpdateHavePieces{PieceIDs: []int32{0}}}); err == nil {
		t.Fatal("old session update accepted after reset")
	}
}

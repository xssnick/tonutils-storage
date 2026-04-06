package storage

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestStoragePeerTouchRefreshesActivity(t *testing.T) {
	tor := &Torrent{
		peers: map[string]*PeerInfo{},
		wake:  newWakeSig(),
	}

	peer := &storagePeer{
		torrent:  tor,
		nodeId:   []byte{0x01},
		nodeAddr: "127.0.0.1:1",
	}

	old := time.Now().Add(-time.Hour).UnixMilli()
	atomic.StoreInt64(&peer.lastActivityAt, old)

	peer.touch()

	if got := atomic.LoadInt64(&peer.lastActivityAt); got <= old {
		t.Fatalf("expected touch to refresh activity timestamp, got %d <= %d", got, old)
	}

	if tor.GetPeer(peer.nodeId) == nil {
		t.Fatal("expected touch to register peer in torrent map")
	}
}

func TestStoragePeerIsIdleFallsBackToSessionInitAt(t *testing.T) {
	peer := &storagePeer{}
	atomic.StoreInt64(&peer.sessionInitAt, time.Now().Add(-peerIdleTimeout-time.Minute).UnixMilli())

	if !peer.isIdle(time.Now(), peerIdleTimeout) {
		t.Fatal("expected session init timestamp to be used when activity timestamp is absent")
	}
}

func TestStoragePeerIsIdleKeepsRecentPeersActive(t *testing.T) {
	peer := &storagePeer{}
	atomic.StoreInt64(&peer.lastActivityAt, time.Now().Add(-time.Minute).UnixMilli())

	if peer.isIdle(time.Now(), peerIdleTimeout) {
		t.Fatal("expected recent peer activity to prevent idle cleanup")
	}
}

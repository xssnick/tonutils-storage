package storage

import (
	"sync"
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

func TestTorrentUpdateUploadedPeerBatchesPersistentStats(t *testing.T) {
	oldBytes := UploadStatsFlushBytes
	oldInterval := UploadStatsFlushInterval
	defer func() {
		UploadStatsFlushBytes = oldBytes
		UploadStatsFlushInterval = oldInterval
	}()

	UploadStatsFlushBytes = 10
	UploadStatsFlushInterval = time.Hour

	store := &recordingStorage{}
	tor := &Torrent{
		BagID: make([]byte, 32),
		db:    store,
		peers: map[string]*PeerInfo{},
		wake:  newWakeSig(),
	}
	tor.SetUploadStats(0)

	peer := &storagePeer{
		torrent:  tor,
		nodeId:   []byte{0x01},
		nodeAddr: "127.0.0.1:1",
	}

	tor.UpdateUploadedPeer(peer, 3)
	tor.UpdateUploadedPeer(peer, 4)
	if len(store.uploadStats) != 0 {
		t.Fatalf("expected small upload updates to stay in memory, got %v", store.uploadStats)
	}

	tor.UpdateUploadedPeer(peer, 3)
	if len(store.uploadStats) != 1 || store.uploadStats[0] != 10 {
		t.Fatalf("expected threshold flush at 10 bytes, got %v", store.uploadStats)
	}

	tor.UpdateUploadedPeer(peer, 1)
	if len(store.uploadStats) != 1 {
		t.Fatalf("expected post-threshold small update to stay batched, got %v", store.uploadStats)
	}

	tor.flushUploadStats()
	if len(store.uploadStats) != 2 || store.uploadStats[1] != 11 {
		t.Fatalf("expected forced flush of final upload stats, got %v", store.uploadStats)
	}
}

func TestSpeedInfoReactsQuicklyAndHoldsBriefGaps(t *testing.T) {
	var s speedInfo
	start := time.Unix(0, 0)
	const bytesPerSecond = 10 << 20

	s.calculateAt(0, start)

	var got float64
	for sec := 1; sec <= 3; sec++ {
		got = s.calculateAt(uint64(sec*bytesPerSecond), start.Add(time.Duration(sec)*time.Second))
	}
	if got < float64(6<<20) {
		t.Fatalf("expected displayed speed to ramp quickly, got %.0f bytes/s", got)
	}

	beforeGap := got
	afterBriefGap := s.calculateAt(3*bytesPerSecond, start.Add(4*time.Second))
	if afterBriefGap < beforeGap*0.95 {
		t.Fatalf("expected brief no-progress gap to be held, got %.0f after %.0f", afterBriefGap, beforeGap)
	}

	afterIdle := s.calculateAt(3*bytesPerSecond, start.Add(12*time.Second))
	if afterIdle >= afterBriefGap {
		t.Fatalf("expected longer idle period to decay speed, got %.0f after %.0f", afterIdle, afterBriefGap)
	}
}

func TestSpeedInfoResetsWhenCounterDrops(t *testing.T) {
	var s speedInfo
	start := time.Unix(0, 0)

	s.calculateAt(1000, start)
	s.calculateAt(2000, start.Add(time.Second))

	if got := s.calculateAt(10, start.Add(2*time.Second)); got != 0 {
		t.Fatalf("expected counter reset to clear displayed speed, got %.0f", got)
	}
}

func TestSpeedInfoDisplayConcurrentWithCalculate(t *testing.T) {
	var s speedInfo
	start := time.Unix(0, 0)
	s.calculateAt(0, start)

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 1; i <= 1000; i++ {
			s.calculateAt(uint64(i*1024), start.Add(time.Duration(i)*time.Millisecond))
		}
	})
	wg.Go(func() {
		for range 1000 {
			_ = s.display()
		}
	})
	wg.Wait()

	if got := s.display(); got <= 0 {
		t.Fatalf("expected published speed to be positive, got %.0f", got)
	}
}

func TestStoragePeerIsIdleFallsBackToSessionInitAt(t *testing.T) {
	peer := &storagePeer{}
	atomic.StoreInt64(&peer.session.sessionInitAt, time.Now().Add(-peerIdleTimeout-time.Minute).UnixMilli())

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

func TestStoragePeerHasPieceLockedWorksWithPendingWriter(t *testing.T) {
	peer := &storagePeer{
		hasPiecesNum: 8,
		hasPieces:    []byte{1 << 3},
	}

	peer.piecesMx.RLock()
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		peer.piecesMx.Lock()
		peer.piecesMx.Unlock()
		close(writerDone)
	}()
	<-writerStarted
	time.Sleep(10 * time.Millisecond)

	got := peer.hasPieceLocked(3)
	peer.piecesMx.RUnlock()

	if !got {
		t.Fatal("expected piece to be visible while read lock is held")
	}

	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire pieces lock after read lock was released")
	}
}

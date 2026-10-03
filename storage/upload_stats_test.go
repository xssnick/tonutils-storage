package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type orderedUploadStatsStorage struct {
	failingStorage
	started   chan struct{}
	proceed   chan struct{}
	persisted atomic.Uint64
	fail      atomic.Bool
}

func (s *orderedUploadStatsStorage) UpdateUploadStats(_ []byte, val uint64) error {
	if s.fail.Load() {
		return errors.New("storage unavailable")
	}
	if val == 10 && s.started != nil {
		close(s.started)
		<-s.proceed
	}
	s.persisted.Store(val)
	return nil
}

func TestUploadStatsSerializePersistentWrites(t *testing.T) {
	oldBytes, oldInterval := UploadStatsFlushBytes, UploadStatsFlushInterval
	defer func() { UploadStatsFlushBytes, UploadStatsFlushInterval = oldBytes, oldInterval }()
	UploadStatsFlushBytes, UploadStatsFlushInterval = 10, time.Hour
	store := &orderedUploadStatsStorage{started: make(chan struct{}), proceed: make(chan struct{})}
	tor := &Torrent{BagID: []byte("bag"), db: store, peers: map[string]*PeerInfo{}, wake: newWakeSig()}
	tor.SetUploadStats(0)
	peer := &storagePeer{torrent: tor, nodeId: []byte("peer")}
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() { tor.UpdateUploadedPeer(peer, 10); close(firstDone) }()
	<-store.started
	go func() { tor.UpdateUploadedPeer(peer, 10); close(secondDone) }()
	for tor.GetUploadStats() != 20 {
		time.Sleep(time.Millisecond)
	}
	close(store.proceed)
	<-firstDone
	<-secondDone
	tor.flushUploadStats()
	if got := store.persisted.Load(); got != 20 {
		t.Fatalf("persisted %d, want 20", got)
	}
}

func TestUploadStatsFailedWriteCanBeRetried(t *testing.T) {
	store := &orderedUploadStatsStorage{}
	store.fail.Store(true)
	tor := &Torrent{BagID: []byte("bag"), db: store, peers: map[string]*PeerInfo{}, wake: newWakeSig()}
	peer := &storagePeer{torrent: tor, nodeId: []byte("peer")}
	tor.UpdateUploadedPeer(peer, 10)
	if stored := atomic.LoadUint64(&tor.stats.StoredUploaded); stored != 0 {
		t.Fatalf("failed write marked %d persisted", stored)
	}
	store.fail.Store(false)
	tor.flushUploadStats()
	if got := store.persisted.Load(); got != 10 {
		t.Fatalf("retry persisted %d, want 10", got)
	}
}

func TestUploadStatsLateCompletionDoesNotReviveClosedPeer(t *testing.T) {
	store := &orderedUploadStatsStorage{}
	tor := &Torrent{BagID: []byte("bag"), db: store, peers: map[string]*PeerInfo{}, wake: newWakeSig()}
	tor.SetUploadStats(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	peer := &storagePeer{torrent: tor, nodeId: []byte("peer"), closerCtx: ctx}
	tor.UpdateUploadedPeer(peer, 7)
	if tor.GetPeer(peer.nodeId) != nil {
		t.Fatal("late completion revived a closed peer")
	}
	if tor.GetUploadStats() != 7 || store.persisted.Load() != 7 {
		t.Fatal("late completion was not counted and persisted")
	}
}

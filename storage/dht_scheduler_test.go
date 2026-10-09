package storage

import (
	"container/heap"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type schedulerStorage struct {
	Storage
	torrents []*Torrent
}

func (s *schedulerStorage) GetAll() []*Torrent { return s.torrents }

type blockedSchedulerDHT struct {
	recordingDHT
	downloadStarted chan struct{}
	seedStarted     chan struct{}
}

func (d *blockedSchedulerDHT) FindOverlayNodes(ctx context.Context, _ []byte, _ ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error) {
	d.downloadStarted <- struct{}{}
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (d *blockedSchedulerDHT) StoreOverlayNodes(ctx context.Context, _ []byte, _ *overlay.NodesList, _ time.Duration) (int, []byte, error) {
	d.seedStarted <- struct{}{}
	<-ctx.Done()
	return 0, nil, ctx.Err()
}

func TestPeerSearcherRefreshesSeedsWithBothDownloadLanesOccupied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	d := &blockedSchedulerDHT{downloadStarted: make(chan struct{}, 40), seedStarted: make(chan struct{}, 5000)}
	store := &schedulerStorage{}
	for i := range 5040 {
		id := make([]byte, 32)
		binary.BigEndian.PutUint32(id, uint32(i))
		torrent := &Torrent{BagID: id, globalCtx: ctx, activeUpload: true, Info: &TorrentInfo{FileSize: 1, PieceSize: 1}}
		torrent.downloadedPiecesCountSet.Store(true)
		if i >= 40 {
			torrent.downloadedPieces.Store(1)
		}
		store.torrents = append(store.torrents, torrent)
	}
	srv := &Server{key: key, dht: d, store: store, closeCtx: ctx, dhtParallelism: 20, startedAt: time.Now().Add(-5 * time.Minute)}
	done := make(chan struct{})
	go func() { srv.startPeerSearcher(); close(done) }()
	defer func() { cancel(); <-done }()
	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	for range 40 {
		select {
		case <-d.downloadStarted:
		case <-deadline.C:
			t.Fatal("downloads failed to occupy both lanes")
		}
	}
	// No download job can complete. Seeds must nevertheless launch on three passes.
	for range 3 * bagDHTSeedLaunchBudget(5000, bagDHTSeedParallelism(5000, 20, 3*time.Second)) {
		select {
		case <-d.seedStarted:
		case <-deadline.C:
			t.Fatal("seed refreshes were starved by the occupied download lanes")
		}
	}
}

func TestDHTSeedsRefreshUnderContinuousDownloadBacklog(t *testing.T) {
	const seedCount = 5000
	const storeTime = 3 * time.Second
	start := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	pool := newDHTJobPool(20)
	// Downloads occupy both old lanes throughout the entire simulation.
	for range 40 {
		if _, ok := pool.tryAcquire(dhtPriorityJob); !ok {
			t.Fatal("failed to fill download workers")
		}
	}
	if _, ok := pool.tryAcquire(dhtPriorityJob); ok {
		t.Fatal("download lanes should be full")
	}
	torrents := make([]*Torrent, seedCount)
	lastSuccess := make([]int64, seedCount)
	lastCompleted := make([]int64, seedCount)
	refreshes := make([]int, seedCount)
	busy := make([]bool, seedCount)
	for i := range torrents {
		id := make([]byte, 32)
		binary.BigEndian.PutUint32(id, uint32(i))
		torrents[i] = &Torrent{BagID: id}
	}
	type pendingStore struct {
		index   int
		started time.Time
		done    time.Time
		release func(time.Duration)
	}
	var pending []pendingStore
	var maxAge time.Duration
	for now := start; !now.After(start.Add(20 * time.Minute)); now = now.Add(peerSearcherInterval) {
		remaining := pending[:0]
		for _, job := range pending {
			if now.Before(job.done) {
				remaining = append(remaining, job)
				continue
			}
			if lastSuccess[job.index] != 0 {
				maxAge = max(maxAge, now.Sub(time.Unix(0, lastSuccess[job.index])))
			}
			// Overlay node versions are stamped before Store starts, rather than
			// when its acknowledgements arrive.
			lastSuccess[job.index] = job.started.UnixNano()
			lastCompleted[job.index] = now.UnixNano()
			refreshes[job.index]++
			busy[job.index] = false
			job.release(storeTime)
		}
		pending = remaining
		q := make(dhtSeedQueue, 0, seedCount)
		for i, torrent := range torrents {
			if busy[i] {
				continue
			}
			due := bagDHTSeedNextAttemptAt(start, torrent.BagID, lastCompleted[i], lastSuccess[i], false, 0)
			if !now.Before(due) {
				q = append(q, dhtSeedCandidate{torrent: torrent, dueAt: due})
			}
		}
		heap.Init(&q)
		capacity := pool.resizeSeeds(seedCount)
		budget := bagDHTSeedLaunchBudget(seedCount, capacity)
		for launched := 0; q.Len() > 0 && launched < budget; launched++ {
			release, ok := pool.tryAcquire(dhtSeedJob)
			if !ok {
				break
			}
			candidate := q.next()
			i := int(binary.BigEndian.Uint32(candidate.torrent.BagID))
			busy[i] = true
			pending = append(pending, pendingStore{index: i, started: now, done: now.Add(storeTime), release: release})
		}
	}
	for i, count := range refreshes {
		if count < 3 {
			t.Fatalf("seed %d only got %d refreshes despite dedicated capacity", i, count)
		}
	}
	if maxAge >= bagDHTOverlayNodeFreshFor {
		t.Fatalf("oldest record age %s exceeds freshness %s", maxAge, bagDHTOverlayNodeFreshFor)
	}
	t.Logf("5000 seeds, 40 continuously occupied download workers, Store=3s: seed workers=%d, maximum published node age=%s (simulation)", pool.seedParallelism, maxAge)
}

type timedSeedStoreRequest struct {
	started time.Time
	version int32
}

type timedSeedSchedulerDHT struct {
	recordingDHT
	started chan timedSeedStoreRequest
	latency time.Duration
}

func (d *timedSeedSchedulerDHT) StoreOverlayNodes(ctx context.Context, _ []byte, nodes *overlay.NodesList, _ time.Duration) (int, []byte, error) {
	request := timedSeedStoreRequest{started: time.Now(), version: nodes.List[0].Version}
	select {
	case d.started <- request:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	timer := time.NewTimer(d.latency)
	defer timer.Stop()
	select {
	case <-timer.C:
		return 3, nil, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func TestPeerSearcherRefreshClockUsesNodeCreationOnSlowStores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		_, key, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		const latency = 40 * time.Second
		fake := &timedSeedSchedulerDHT{started: make(chan timedSeedStoreRequest, 4), latency: latency}
		torrent := &Torrent{
			BagID: make([]byte, 32), globalCtx: ctx, activeUpload: true,
			Info: &TorrentInfo{FileSize: 1, PieceSize: 1},
		}
		torrent.downloadedPiecesCountSet.Store(true)
		torrent.downloadedPieces.Store(1)
		server := &Server{
			key: key, dht: fake, store: &schedulerStorage{torrents: []*Torrent{torrent}},
			closeCtx: ctx, dhtParallelism: 1, startedAt: time.Now().Add(-5 * time.Minute),
		}
		done := make(chan struct{})
		go func() { server.startPeerSearcher(); close(done) }()
		first := <-fake.started
		time.Sleep(latency)
		synctest.Wait()
		successAt := atomic.LoadInt64(&torrent.lastDHTStoreSuccessAt)
		if successAt != first.started.UnixNano() || int64(first.version) != first.started.Unix() {
			t.Fatalf("refresh clock=%s, node created=%s, version=%d", time.Unix(0, successAt), first.started, first.version)
		}
		second := <-fake.started
		if gap := second.started.Sub(first.started); gap != bagDHTSeedRefreshInterval {
			t.Fatalf("slow Store acknowledgements inflated refresh interval to %s, want %s", gap, bagDHTSeedRefreshInterval)
		}
		// The previous node remains authoritative until the new Store completes.
		maxPublishedAge := second.started.Add(latency).Sub(first.started)
		if maxPublishedAge != bagDHTSeedRefreshInterval+latency {
			t.Fatalf("published node age=%s, want %s", maxPublishedAge, bagDHTSeedRefreshInterval+latency)
		}
		cancel()
		<-done
		synctest.Wait()
		t.Logf("one seed, Store=40s: refresh starts every %s, maximum published node age=%s", bagDHTSeedRefreshInterval, maxPublishedAge)
	})
}

func TestPeerSearcherPublishesNewLocalSeedWithoutStartupSpread(t *testing.T) {
	var lateBag []byte
	for i := range 10000 {
		id := make([]byte, 32)
		binary.BigEndian.PutUint32(id, uint32(i))
		if offset := bagDHTSeedSpreadOffset(id, bagDHTSeedInitialSpreadWindow); offset >= bagDHTSeedInitialSpreadWindow-10*time.Second {
			lateBag = id
			break
		}
	}
	if lateBag == nil {
		t.Fatal("could not find a BagID near the end of the startup spread")
	}
	tests := []struct {
		name           string
		createdLocally bool
		age            time.Duration
		immediate      bool
	}{
		{name: "new local seed", createdLocally: true, immediate: true},
		{name: "downloaded startup seed"},
		{name: "older local startup seed", createdLocally: true, age: 16 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				_, key, err := ed25519.GenerateKey(nil)
				if err != nil {
					t.Fatal(err)
				}
				startedAt := time.Now()
				fake := &timedSeedSchedulerDHT{started: make(chan timedSeedStoreRequest, 4), latency: time.Second}
				torrent := &Torrent{
					BagID: lateBag, globalCtx: ctx, activeUpload: true,
					Info:           &TorrentInfo{FileSize: 1, PieceSize: 1},
					CreatedLocally: test.createdLocally, CreatedAt: startedAt.Add(-test.age),
				}
				torrent.downloadedPiecesCountSet.Store(true)
				torrent.downloadedPieces.Store(1)
				server := &Server{
					key: key, dht: fake, store: &schedulerStorage{torrents: []*Torrent{torrent}},
					closeCtx: ctx, dhtParallelism: 1, startedAt: startedAt,
				}
				done := make(chan struct{})
				go func() { server.startPeerSearcher(); close(done) }()
				synctest.Wait()
				time.Sleep(peerSearcherInterval)
				synctest.Wait()
				var request timedSeedStoreRequest
				if test.immediate {
					select {
					case request = <-fake.started:
					default:
						t.Fatal("new local seed did not publish on the first scheduler tick")
					}
					if request.started.Sub(startedAt) != peerSearcherInterval {
						t.Fatalf("first local publication started after %s", request.started.Sub(startedAt))
					}
				} else {
					select {
					case <-fake.started:
						t.Fatal("ordinary startup seed bypassed its spread")
					default:
					}
					request = <-fake.started
					spread := bagDHTSeedSpreadOffset(lateBag, bagDHTSeedInitialSpreadWindow)
					if delay := request.started.Sub(startedAt); delay < spread || delay > spread+peerSearcherInterval {
						t.Fatalf("startup publication delay=%s, want spread=%s plus one tick", delay, spread)
					}
				}
				cancel()
				<-done
				synctest.Wait()
			})
		})
	}
}

func TestDHTSeedDeadlineOrder(t *testing.T) {
	now := time.Now()
	q := dhtSeedQueue{
		{torrent: &Torrent{BagID: []byte{3}}, dueAt: now},
		{torrent: &Torrent{BagID: []byte{2}}, dueAt: now.Add(-time.Minute)},
		{torrent: &Torrent{BagID: []byte{1}}, dueAt: now.Add(-time.Minute)},
	}
	heap.Init(&q)
	for _, want := range []byte{1, 2, 3} {
		if got := q.next().torrent.BagID[0]; got != want {
			t.Fatalf("deadline queue returned %d, want %d", got, want)
		}
	}
}

func TestDHTSeedCapacityTracksSlowStoresAndStaysBounded(t *testing.T) {
	pool := newDHTJobPool(20)
	initial := pool.resizeSeeds(5000)
	release, ok := pool.tryAcquire(dhtSeedJob)
	if !ok {
		t.Fatal("no seed capacity")
	}
	release(9 * time.Second)
	slow := pool.resizeSeeds(5000)
	if slow <= initial {
		t.Fatalf("capacity did not adapt: %d -> %d", initial, slow)
	}
	if got := bagDHTSeedParallelism(5000, 20, time.Minute); got != bagDHTMaxSeedParallelism {
		t.Fatalf("slow/outage capacity %d should be capped at %d", got, bagDHTMaxSeedParallelism)
	}
}

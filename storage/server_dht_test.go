package storage

import (
	"context"
	"crypto/ed25519"
	"slices"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func TestBagDHTAttemptDelayDoesNotOutliveOverlayFreshness(t *testing.T) {
	tests := []struct {
		name                string
		activeDownload      bool
		numPeers            int
		usableDownloadPeers int
		serverMode          bool
		zeroPeerSearches    uint32
		jitter              time.Duration
	}{
		{
			name:     "upload with peers",
			numPeers: 12,
			jitter:   bagDHTSteadyRefreshJitter,
		},
		{
			name:       "server seed without peers",
			serverMode: true,
			jitter:     bagDHTSteadyRefreshJitter,
		},
		{
			name:             "client zero peer backoff cap",
			zeroPeerSearches: 100,
			jitter:           bagDHTZeroPeerRetryJitter,
		},
		{
			name:                "active download with usable peers",
			activeDownload:      true,
			numPeers:            4,
			usableDownloadPeers: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := bagDHTAttemptDelay(
				tt.activeDownload,
				tt.numPeers,
				tt.usableDownloadPeers,
				tt.serverMode,
				tt.zeroPeerSearches,
				tt.jitter,
			)
			if delay >= bagDHTOverlayNodeFreshFor {
				t.Fatalf("bag dht delay %s should stay below overlay freshness %s", delay, bagDHTOverlayNodeFreshFor)
			}
		})
	}
}

func TestBagDHTNextAttemptUsesCompletionTime(t *testing.T) {
	attemptAt := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	completedAt := attemptAt.Add(4 * time.Minute)

	got := bagDHTNextAttemptAt(
		attemptAt.UnixNano(),
		completedAt.UnixNano(),
		false,
		1,
		0,
		false,
		0,
		0,
	)
	want := completedAt.Add(bagDHTSteadyRefreshInterval)
	if !got.Equal(want) {
		t.Fatalf("next attempt should be based on completed time, got %s want %s", got, want)
	}
}

func TestBagDHTSeedNextAttemptUsesSuccessAndFastFailureRetry(t *testing.T) {
	startedAt := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	successAt := startedAt.Add(2 * time.Minute)
	completedAt := successAt.Add(bagDHTSeedRefreshInterval)

	got := bagDHTSeedNextAttemptAt(startedAt, []byte{1}, completedAt.UnixNano(), successAt.UnixNano(), false, 0)
	want := successAt.Add(bagDHTSeedRefreshInterval)
	if !got.Equal(want) {
		t.Fatalf("seed refresh should be based on last successful store, got %s want %s", got, want)
	}

	got = bagDHTSeedNextAttemptAt(startedAt, []byte{1}, completedAt.UnixNano(), successAt.UnixNano(), true, 1)
	want = completedAt.Add(bagDHTSeedRetryBaseInterval)
	if !got.Equal(want) {
		t.Fatalf("failed seed refresh should retry quickly, got %s want %s", got, want)
	}
}

func TestBagDHTSeedInitialSpreadAndBudgetCoverLargeSeedSet(t *testing.T) {
	startedAt := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	bag := make([]byte, 32)
	bag[0] = 7

	got := bagDHTSeedNextAttemptAt(startedAt, bag, 0, 0, false, 0)
	if got.Before(startedAt) || !got.Before(startedAt.Add(bagDHTSeedInitialSpreadWindow)) {
		t.Fatalf("initial seed refresh should be spread inside startup window, got %s", got)
	}

	seedCount := 2800
	budget := bagDHTSeedLaunchBudget(seedCount, 50)
	if budget <= 0 {
		t.Fatal("launch budget should be positive")
	}
	if budget*int(bagDHTSeedRefreshInterval/time.Second) < seedCount {
		t.Fatalf("budget %d/sec cannot cover %d seeds in %s", budget, seedCount, bagDHTSeedRefreshInterval)
	}
}

func TestStoreBagDHTSelfStoresSingleLocalNodeWithoutFind(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	bag := make([]byte, 32)
	bag[0] = 1

	dht := &recordingDHT{}
	srv := &Server{
		key: key,
		dht: dht,
	}

	stored, err := srv.storeBagDHTSelf(context.Background(), &Torrent{BagID: bag})
	if err != nil {
		t.Fatalf("storeBagDHTSelf failed: %v", err)
	}
	if stored != 1 {
		t.Fatalf("unexpected stored count: %d", stored)
	}
	if dht.findOverlayCalls != 0 {
		t.Fatalf("store-only refresh should not call FindOverlayNodes, got %d calls", dht.findOverlayCalls)
	}
	if dht.storeOverlayCalls != 1 {
		t.Fatalf("expected one StoreOverlayNodes call, got %d", dht.storeOverlayCalls)
	}
	if !slices.Equal(dht.overlayKey, bag) {
		t.Fatal("StoreOverlayNodes got wrong overlay key")
	}
	if dht.ttl != 45*time.Minute {
		t.Fatalf("unexpected ttl: %s", dht.ttl)
	}
	if dht.nodes == nil || len(dht.nodes.List) != 1 {
		t.Fatalf("expected exactly one local overlay node, got %#v", dht.nodes)
	}
}

type recordingDHT struct {
	storeOverlayCalls int
	findOverlayCalls  int
	overlayKey        []byte
	nodes             *overlay.NodesList
	ttl               time.Duration
}

func (r *recordingDHT) StoreAddress(context.Context, address.List, time.Duration, ed25519.PrivateKey) (int, []byte, error) {
	return 0, nil, nil
}

func (r *recordingDHT) StoreOverlayNodes(_ context.Context, overlayKey []byte, nodes *overlay.NodesList, ttl time.Duration) (int, []byte, error) {
	r.storeOverlayCalls++
	r.overlayKey = append([]byte(nil), overlayKey...)
	if nodes != nil {
		cp := *nodes
		cp.List = append([]overlay.Node(nil), nodes.List...)
		r.nodes = &cp
	}
	r.ttl = ttl
	return 1, nil, nil
}

func (r *recordingDHT) FindAddresses(context.Context, []byte) (*address.List, ed25519.PublicKey, error) {
	return nil, nil, nil
}

func (r *recordingDHT) FindOverlayNodes(context.Context, []byte, ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error) {
	r.findOverlayCalls++
	return nil, nil, dht.ErrDHTValueIsNotFound
}

func (r *recordingDHT) Close() {}

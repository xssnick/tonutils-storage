package storage

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func TestDownloadPrefetchPiecesUsesByteWindow(t *testing.T) {
	oldPrefetch := DownloadPrefetch
	oldMin := DownloadPrefetchMinPieces
	defer func() {
		DownloadPrefetch = oldPrefetch
		DownloadPrefetchMinPieces = oldMin
	}()

	DownloadPrefetch = 0
	DownloadPrefetchMinPieces = 4

	if got := downloadPrefetchPieces(1<<20, 64<<20); got != 64 {
		t.Fatalf("expected 64 pieces for 64 MB window and 1 MB pieces, got %d", got)
	}
	if got := downloadPrefetchPieces(8<<20, 64<<20); got != 8 {
		t.Fatalf("expected 8 pieces for 64 MB window and 8 MB pieces, got %d", got)
	}
	if got := downloadPrefetchPieces(32<<20, 64<<20); got != 4 {
		t.Fatalf("expected minimum 4 pieces for large pieces, got %d", got)
	}

	DownloadPrefetch = 5
	if got := downloadPrefetchPieces(1<<20, 64<<20); got != 5 {
		t.Fatalf("expected explicit piece cap to limit prefetch to 5, got %d", got)
	}
}

func TestEffectiveMaxInflightPiecesCapsToDataQueue(t *testing.T) {
	conn := &PeerConnection{
		dataQueue: make(chan struct{}, 3),
	}
	conn.MaxInflightPieces.Store(10)
	if got := effectiveMaxInflightPieces(conn); got != 3 {
		t.Fatalf("expected data queue cap 3, got %d", got)
	}

	conn.MaxInflightPieces.Store(2)
	if got := effectiveMaxInflightPieces(conn); got != 2 {
		t.Fatalf("expected configured max 2 below data queue cap, got %d", got)
	}

	conn.MaxInflightPieces.Store(0)
	if got := effectiveMaxInflightPieces(conn); got != 1 {
		t.Fatalf("expected minimum max inflight 1, got %d", got)
	}
}

func TestPreFetcherWindowCursorReservesEarlyUnavailablePieces(t *testing.T) {
	fetch := &PreFetcher{
		piecesList: []byte{1, 1, 1, 1},
	}
	peer := testPeerWithPieces(4, 1<<3)

	if piece, ok := fetch.nextPieceForPeer(peer, 1); ok {
		t.Fatalf("expected early unavailable piece to reserve the only slot, got piece %d", piece)
	}
	if fetch.pieceCursor != 0 {
		t.Fatalf("expected cursor to stay at earliest unavailable piece, got %d", fetch.pieceCursor)
	}

	piece, ok := fetch.nextPieceForPeer(peer, 4)
	if !ok || piece != 3 {
		t.Fatalf("expected piece 3 once window has room for earlier holes, got piece=%d ok=%v", piece, ok)
	}
}

func TestPreFetcherWindowCursorRewindsFailedPiece(t *testing.T) {
	fetch := &PreFetcher{
		pieceCursor: 2,
		piecesList:  []byte{2, 2},
	}
	peer := testPeerWithPieces(2, 1)

	fetch.markPieceForRetry(0)

	if fetch.pieceCursor != 0 {
		t.Fatalf("expected retry to rewind cursor to failed piece, got %d", fetch.pieceCursor)
	}
	piece, ok := fetch.nextPieceForPeer(peer, 1)
	if !ok || piece != 0 {
		t.Fatalf("expected failed piece 0 to be reachable after cursor rewind, got piece=%d ok=%v", piece, ok)
	}
}

func TestUnorderedPreFetcherRoundRobinWrapsAndRetriesPieces(t *testing.T) {
	fetch := &PreFetcher{
		pieceCursor: 2,
		roundRobin:  true,
		piecesList:  []byte{1, 1, 1, 1},
	}
	peer := testPeerWithPieces(4, 1<<0|1<<2)

	piece, ok := fetch.nextPieceForPeer(peer, 1)
	if !ok || piece != 2 {
		t.Fatalf("expected round-robin to start at cursor and pick piece 2, got piece=%d ok=%v", piece, ok)
	}

	fetch.piecesList[piece] = 2
	piece, ok = fetch.nextPieceForPeer(peer, 1)
	if !ok || piece != 0 {
		t.Fatalf("expected round-robin to wrap and pick piece 0, got piece=%d ok=%v", piece, ok)
	}

	fetch.piecesList[2] = 1
	piece, ok = fetch.nextPieceForPeer(peer, 1)
	if !ok || piece != 2 {
		t.Fatalf("expected failed piece 2 to remain reachable after retry, got piece=%d ok=%v", piece, ok)
	}
}

func TestPreFetcherStopDuringThrottleReleasesReservations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &Server{}
	srv.downloadMaxInflight.Store(1)

	conn := &PeerConnection{
		srv:       srv,
		dataQueue: make(chan struct{}, 1),
	}
	conn.MaxInflightPieces.Store(3)

	torrent := &Torrent{
		Info:      &TorrentInfo{PieceSize: 4096, FileSize: 4096, HeaderSize: 1},
		peers:     map[string]*PeerInfo{},
		wake:      newWakeSig(),
		connector: alwaysLimitedConnector{},
	}
	peer := testPeerWithPieces(1, 1)
	peer.torrent = torrent
	peer.conn = conn
	peer.nodeId = []byte("peer")
	torrent.peers["peer"] = &PeerInfo{peer: peer}

	fetch := NewPreFetcher(ctx, torrent, nil, 1, []byte{1})
	defer fetch.Stop()

	waitForTestCondition(t, time.Second, func() bool {
		return fetch.ready.Load() == 1 &&
			conn.InflightPieces.Load() == 1 &&
			srv.downloadInflight.Load() == 1
	}, "prefetch reservations")

	fetch.Stop()

	waitForTestCondition(t, time.Second, func() bool {
		return fetch.ready.Load() == 0 &&
			conn.InflightPieces.Load() == 0 &&
			srv.downloadInflight.Load() == 0
	}, "released prefetch reservations")
}

func testPeerWithPieces(piecesNum uint32, firstByte byte) *storagePeer {
	peer := &storagePeer{}
	peer.resetPieceTrackingLocked(piecesNum)
	if len(peer.hasPieces) > 0 {
		peer.hasPieces[0] = firstByte
	}
	peer.knownPieces = countBitsetOnes(peer.hasPieces)
	peer.session.localInitSent = 1
	peer.session.remoteInitComplete = 1
	return peer
}

func waitForTestCondition(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type alwaysLimitedConnector struct{}

func (alwaysLimitedConnector) GetID() []byte {
	return []byte("local")
}

func (alwaysLimitedConnector) GetADNLPrivateKey() ed25519.PrivateKey {
	return nil
}

func (alwaysLimitedConnector) SetDownloadLimit(uint64) {}

func (alwaysLimitedConnector) SetUploadLimit(uint64) {}

func (alwaysLimitedConnector) GetUploadLimit() uint64 {
	return 0
}

func (alwaysLimitedConnector) GetDownloadLimit() uint64 {
	return 0
}

func (alwaysLimitedConnector) ThrottleDownload(ctx context.Context, _ uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("limited")
	}
}

func (alwaysLimitedConnector) ThrottleUpload(context.Context, uint64) error {
	return nil
}

func (alwaysLimitedConnector) CreateDownloader(context.Context, *Torrent) (TorrentDownloader, error) {
	return nil, nil
}

func (alwaysLimitedConnector) ConnectToNode(context.Context, *Torrent, *overlay.Node, *address.List) error {
	return nil
}

func (alwaysLimitedConnector) Stop() {}

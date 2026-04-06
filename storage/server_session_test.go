package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
)

func TestPrepareStoragePeer_ReusesHealthyOutgoingSession(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
	}

	conn := &PeerConnection{
		usedByBags: map[string]*storagePeer{},
	}

	peer := &storagePeer{
		torrent:     tor,
		nodeId:      []byte("node"),
		conn:        conn,
		sessionId:   111,
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() {},
	}
	atomic.StoreInt32(&peer.sessionInitialized, 1)
	atomic.StoreInt32(&peer.updateInitReceived, 1)
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionCtx := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected healthy session to be reused")
	}
	if sessionCtx != nil {
		t.Fatal("expected healthy session reuse without reinitialization")
	}
	if atomic.LoadInt64(&peer.sessionId) != 111 {
		t.Fatal("expected session id to stay intact")
	}
}

func TestPrepareStoragePeer_ReinitializesUnhealthyOutgoingSession(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
		Info: &TorrentInfo{
			PieceSize:  1,
			FileSize:   64,
			HeaderSize: 1,
		},
	}

	conn := &PeerConnection{
		usedByBags: map[string]*storagePeer{},
	}

	stopCalls := int32(0)
	peer := &storagePeer{
		torrent:            tor,
		nodeId:             []byte("node"),
		conn:               conn,
		sessionId:          222,
		closerCtx:          context.Background(),
		stop:               func() {},
		hasPieces:          []byte{0xff, 0xff},
		hasPiecesNum:       16,
		initExpectedChunks: 2,
		initReceivedChunks: 1,
		initChunksMask:     []byte{0x01},
		pendingInitChunks:  map[uint32][]byte{0: {0x01}},
		pendingInitBytes:   1,
		pendingHavePieces:  []int32{2, 3},
		stopSession: func() {
			atomic.AddInt32(&stopCalls, 1)
		},
	}
	atomic.StoreInt32(&peer.sessionInitialized, 0)
	atomic.StoreInt32(&peer.updateInitReceived, 0)
	atomic.StoreInt64(&peer.sessionInitAt, time.Now().Add(-2*time.Minute).UnixMilli())
	atomic.StoreInt64(&peer.lastInitChunkAt, time.Now().Add(-50*time.Second).UnixMilli())
	atomic.StoreUint64(&peer.lastSentNewPiecesPos, 33)
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionCtx := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected existing peer to be reinitialized in place")
	}
	if sessionCtx == nil {
		t.Fatal("expected unhealthy session to request reinitialization")
	}
	if atomic.LoadInt32(&stopCalls) != 1 {
		t.Fatal("expected previous session context to be stopped before reinit")
	}
	if atomic.LoadInt64(&peer.sessionId) == 222 {
		t.Fatal("expected a fresh session id for outgoing reinit")
	}
	if atomic.LoadInt32(&peer.sessionInitialized) != 0 {
		t.Fatal("expected sessionInitialized to be reset before reinit")
	}
	if atomic.LoadInt32(&peer.updateInitReceived) != 0 {
		t.Fatal("expected updateInitReceived to be reset before reinit")
	}
	if atomic.LoadInt64(&peer.lastInitChunkAt) != 0 {
		t.Fatal("expected init progress timestamp to be reset before reinit")
	}
	if atomic.LoadUint64(&peer.lastSentNewPiecesPos) != 0 {
		t.Fatal("expected incremental pieces cursor to be reset before reinit")
	}
	if peer.initReceivedChunks != 0 {
		t.Fatal("expected received init chunks to be reset before reinit")
	}
	if peer.initExpectedChunks != initChunksCount(tor.Info.PiecesNum()) {
		t.Fatal("expected init chunk count to be recalculated on reinit")
	}
	if len(peer.hasPieces) != piecesBitsetBytes(tor.Info.PiecesNum()) {
		t.Fatal("expected hasPieces bitset to be resized on reinit")
	}
	if len(peer.initChunksMask) != piecesBitsetBytes(peer.initExpectedChunks) {
		t.Fatal("expected init chunk mask to be reset on reinit")
	}
	if len(peer.pendingInitChunks) != 0 || peer.pendingInitBytes != 0 {
		t.Fatal("expected pending init updates to be cleared on reinit")
	}
	if len(peer.pendingHavePieces) != 0 {
		t.Fatal("expected pending have updates to be cleared on reinit")
	}
}

func TestPrepareStoragePeer_ReusesInitializedPartialOutgoingSession(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
	}

	conn := &PeerConnection{
		usedByBags: map[string]*storagePeer{},
	}

	peer := &storagePeer{
		torrent:     tor,
		nodeId:      []byte("node"),
		conn:        conn,
		sessionId:   333,
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() {},
	}
	atomic.StoreInt32(&peer.sessionInitialized, 1)
	atomic.StoreInt32(&peer.updateInitReceived, 0)
	atomic.StoreUint32(&peer.knownPieces, 12)
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionCtx := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected initialized partial session to be reused")
	}
	if sessionCtx != nil {
		t.Fatal("expected partial session reuse without reinitialization")
	}
	if atomic.LoadInt64(&peer.sessionId) != 333 {
		t.Fatal("expected session id to stay intact")
	}
}

func TestPrepareStoragePeer_ReinitializesInitializedButUnusableOutgoingSession(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
		Info: &TorrentInfo{
			PieceSize:  1,
			FileSize:   64,
			HeaderSize: 1,
		},
	}

	conn := &PeerConnection{
		usedByBags: map[string]*storagePeer{},
	}

	stopCalls := int32(0)
	peer := &storagePeer{
		torrent:     tor,
		nodeId:      []byte("node"),
		conn:        conn,
		sessionId:   444,
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() { atomic.AddInt32(&stopCalls, 1) },
	}
	atomic.StoreInt32(&peer.sessionInitialized, 1)
	atomic.StoreInt32(&peer.updateInitReceived, 0)
	atomic.StoreUint32(&peer.knownPieces, 0)
	atomic.StoreInt64(&peer.sessionInitAt, time.Now().Add(-2*time.Minute).UnixMilli())
	atomic.StoreInt64(&peer.lastInitChunkAt, time.Now().Add(-50*time.Second).UnixMilli())
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionCtx := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected existing peer to be reinitialized in place")
	}
	if sessionCtx == nil {
		t.Fatal("expected unusable timed out session to request reinitialization")
	}
	if atomic.LoadInt32(&stopCalls) != 1 {
		t.Fatal("expected previous session context to be stopped before reinit")
	}
	if atomic.LoadInt64(&peer.sessionId) == 444 {
		t.Fatal("expected a fresh session id for outgoing reinit")
	}
}

func TestStoragePeerApplyInitChunk_CompletesAfterAllChunks(t *testing.T) {
	peer := &storagePeer{}
	piecesNum := uint32(maxPiecesBytesPerRequest*8 + 1)

	firstChunk := bytes.Repeat([]byte{0xff}, maxPiecesBytesPerRequest)
	complete, err := peer.applyInitChunk(piecesNum, 0, firstChunk)
	if err != nil {
		t.Fatalf("unexpected error on first init chunk: %v", err)
	}
	if complete {
		t.Fatal("expected init to stay incomplete after first chunk")
	}
	if peer.hasPiece(piecesNum - 1) {
		t.Fatal("expected last piece to stay unavailable until final chunk")
	}

	complete, err = peer.applyInitChunk(piecesNum, uint32(maxPiecesBytesPerRequest*8), []byte{0x01})
	if err != nil {
		t.Fatalf("unexpected error on final init chunk: %v", err)
	}
	if !complete {
		t.Fatal("expected init to complete after final chunk")
	}
	if !peer.hasPiece(0) {
		t.Fatal("expected first piece bit to be tracked after init chunks")
	}
	if !peer.hasPiece(piecesNum - 1) {
		t.Fatal("expected final piece bit to be tracked after init chunks")
	}
}

func TestStoragePeerInitProgressTimedOut_UsesLastChunkProgress(t *testing.T) {
	peer := &storagePeer{}
	now := time.Now()

	atomic.StoreInt64(&peer.sessionInitAt, now.Add(-2*time.Minute).UnixMilli())
	atomic.StoreInt64(&peer.lastInitChunkAt, now.Add(-10*time.Second).UnixMilli())
	if peer.initProgressTimedOut(now, 45*time.Second) {
		t.Fatal("expected recent init chunk to extend session init timeout")
	}

	atomic.StoreInt64(&peer.lastInitChunkAt, now.Add(-50*time.Second).UnixMilli())
	if !peer.initProgressTimedOut(now, 45*time.Second) {
		t.Fatal("expected timeout when no init progress was observed for too long")
	}
}

type testRLDP struct {
	queries []AddUpdate
	onQuery func(query tl.Serializable, result tl.Serializable) error
}

func (t *testRLDP) GetADNL() rldp.ADNL {
	return nil
}

func (t *testRLDP) GetRateInfo() (left int64, total int64) {
	return 0, 0
}

func (t *testRLDP) Close() {}

func (t *testRLDP) DoQuery(_ context.Context, _ uint64, query, result tl.Serializable) error {
	if t.onQuery != nil {
		if err := t.onQuery(query, result); err != nil {
			return err
		}
	}

	req, _ := overlay.UnwrapQuery(query)
	up, ok := req.(AddUpdate)
	if ok {
		t.queries = append(t.queries, up)
		return nil
	}

	upPtr, ok := req.(*AddUpdate)
	if ok {
		t.queries = append(t.queries, *upPtr)
	}
	return nil
}

func (t *testRLDP) DoQueryAsync(_ context.Context, _ uint64, _ []byte, _ tl.Serializable, _ chan<- rldp.AsyncQueryResult) error {
	return nil
}

func (t *testRLDP) SetOnQuery(_ func([]byte, *rldp.Query) error) {}

func (t *testRLDP) SetOnDisconnect(_ func()) {}

func (t *testRLDP) SendAnswer(_ context.Context, _ uint64, _ uint32, _, _ []byte, _ tl.Serializable) error {
	return nil
}

type testADNLPeer struct {
	queryErrs  []error
	queryCalls atomic.Int32
}

func (t *testADNLPeer) SetCustomMessageHandler(func(msg *adnl.MessageCustom) error) {}

func (t *testADNLPeer) SetQueryHandler(func(msg *adnl.MessageQuery) error) {}

func (t *testADNLPeer) GetDisconnectHandler() func(addr string, key ed25519.PublicKey) {
	return nil
}

func (t *testADNLPeer) SetDisconnectHandler(func(addr string, key ed25519.PublicKey)) {}

func (t *testADNLPeer) SendCustomMessage(context.Context, tl.Serializable) error {
	return nil
}

func (t *testADNLPeer) Query(_ context.Context, _, _ tl.Serializable) error {
	call := int(t.queryCalls.Add(1)) - 1
	if call < len(t.queryErrs) && t.queryErrs[call] != nil {
		return t.queryErrs[call]
	}
	return nil
}

func (t *testADNLPeer) Answer(context.Context, []byte, tl.Serializable) error {
	return nil
}

func (t *testADNLPeer) Ping(context.Context) (time.Duration, error) {
	return 0, nil
}

func (t *testADNLPeer) GetQueryHandler() func(msg *adnl.MessageQuery) error {
	return nil
}

func (t *testADNLPeer) GetCloserCtx() context.Context {
	return context.Background()
}

func (t *testADNLPeer) SetAddresses(address.List) {}

func (t *testADNLPeer) RemoteAddr() string {
	return "127.0.0.1:1"
}

func (t *testADNLPeer) GetID() []byte {
	return []byte("test-peer")
}

func (t *testADNLPeer) GetPubKey() ed25519.PublicKey {
	return nil
}

func (t *testADNLPeer) Reinit() {}

func (t *testADNLPeer) Close() {}

func TestStoragePeerUpdateInitPieces_InitializesDownloadState(t *testing.T) {
	rl := &testRLDP{}
	tor := &Torrent{
		BagID:        []byte("bag"),
		Info:         &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1},
		globalCtx:    context.Background(),
		activeUpload: false,
		pieceMask:    []byte{0b00110101},
	}
	peer := &storagePeer{
		torrent:   tor,
		conn:      &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 2)},
		overlay:   []byte("overlay"),
		sessionId: 77,
	}

	if err := peer.updateInitPieces(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rl.queries) != 1 {
		t.Fatalf("expected single init update, got %d", len(rl.queries))
	}

	upd, ok := rl.queries[0].Update.(UpdateInit)
	if !ok {
		t.Fatalf("expected UpdateInit, got %T", rl.queries[0].Update)
	}
	if upd.State.WantDownload != true {
		t.Fatal("expected download init to advertise WantDownload")
	}
	if upd.State.WillUpload {
		t.Fatal("expected download-only init to keep WillUpload=false")
	}
	if upd.HavePiecesOffset != 0 {
		t.Fatalf("expected first init chunk offset 0, got %d", upd.HavePiecesOffset)
	}
	if !bytes.Equal(upd.HavePieces, tor.pieceMask) {
		t.Fatal("expected init update to send current piece mask")
	}
}

func TestStoragePeerUpdateInitPieces_InitializesUploadStateAndChunksMask(t *testing.T) {
	mask := append(bytes.Repeat([]byte{0xff}, maxPiecesBytesPerRequest), 0x03)
	piecesNum := uint32(len(mask)-1)*8 + 2

	rl := &testRLDP{}
	tor := &Torrent{
		BagID:        []byte("bag"),
		Info:         &TorrentInfo{PieceSize: 1, FileSize: uint64(piecesNum), HeaderSize: 1},
		globalCtx:    context.Background(),
		activeUpload: true,
		pieceMask:    mask,
	}
	peer := &storagePeer{
		torrent:   tor,
		conn:      &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 2)},
		overlay:   []byte("overlay"),
		sessionId: 88,
	}

	if err := peer.updateInitPieces(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rl.queries) != 2 {
		t.Fatalf("expected two init updates for chunked mask, got %d", len(rl.queries))
	}

	first, ok := rl.queries[0].Update.(UpdateInit)
	if !ok {
		t.Fatalf("expected first update to be UpdateInit, got %T", rl.queries[0].Update)
	}
	second, ok := rl.queries[1].Update.(UpdateInit)
	if !ok {
		t.Fatalf("expected second update to be UpdateInit, got %T", rl.queries[1].Update)
	}
	if !first.State.WantDownload || !first.State.WillUpload {
		t.Fatal("expected first init chunk to advertise both download and upload")
	}
	if !second.State.WantDownload || !second.State.WillUpload {
		t.Fatal("expected second init chunk to advertise both download and upload")
	}
	if first.HavePiecesOffset != 0 {
		t.Fatalf("expected first init chunk offset 0, got %d", first.HavePiecesOffset)
	}
	if second.HavePiecesOffset != int32(maxPiecesBytesPerRequest*8) {
		t.Fatalf("expected second init chunk offset %d, got %d", maxPiecesBytesPerRequest*8, second.HavePiecesOffset)
	}
	if len(first.HavePieces) != maxPiecesBytesPerRequest {
		t.Fatalf("expected first chunk size %d, got %d", maxPiecesBytesPerRequest, len(first.HavePieces))
	}
	if len(second.HavePieces) != 1 {
		t.Fatalf("expected second chunk size 1, got %d", len(second.HavePieces))
	}
	if second.HavePieces[0] != 0x03 {
		t.Fatalf("expected tail chunk bits 0x03, got 0x%x", second.HavePieces[0])
	}
}

func TestStoragePeerUpdateInitPieces_UsesDedicatedInitControlQueue(t *testing.T) {
	rl := &testRLDP{}
	controlQueue := make(chan struct{}, 1)
	controlQueue <- struct{}{}

	tor := &Torrent{
		BagID:        []byte("bag"),
		Info:         &TorrentInfo{PieceSize: 4096, FileSize: 4096, HeaderSize: 1},
		globalCtx:    context.Background(),
		activeUpload: false,
		pieceMask:    []byte{0x01},
	}
	peer := &storagePeer{
		torrent: tor,
		conn: &PeerConnection{
			rldp:             rl,
			controlQueue:     controlQueue,
			initControlQueue: make(chan struct{}, 1),
		},
		overlay:   []byte("overlay"),
		sessionId: 111,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := peer.updateInitPieces(ctx); err != nil {
		t.Fatalf("expected init update to succeed while regular control queue is busy: %v", err)
	}
	if len(rl.queries) != 1 {
		t.Fatalf("expected one init update, got %d", len(rl.queries))
	}
}

func TestStoragePeerUpdateHavePieces_UsesIncrementalCursor(t *testing.T) {
	rl := &testRLDP{}
	tor := &Torrent{
		BagID:      []byte("bag"),
		globalCtx:  context.Background(),
		peers:      map[string]*PeerInfo{},
		knownNodes: map[string]*KnownNode{},
	}
	peer := &storagePeer{
		torrent:   tor,
		conn:      &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 2)},
		overlay:   []byte("overlay"),
		sessionId: 99,
		nodeId:    []byte("node"),
	}
	info := &PeerInfo{peer: peer}
	tor.peers[string(peer.nodeId)] = info

	tor.enqueueNewPiece(1)
	tor.enqueueNewPiece(3)
	if err := peer.updateHavePieces(context.Background()); err != nil {
		t.Fatalf("unexpected error on first incremental update: %v", err)
	}
	if len(rl.queries) != 1 {
		t.Fatalf("expected first incremental update to send one batch, got %d", len(rl.queries))
	}
	first, ok := rl.queries[0].Update.(UpdateHavePieces)
	if !ok {
		t.Fatalf("expected UpdateHavePieces, got %T", rl.queries[0].Update)
	}
	if len(first.PieceIDs) != 2 || first.PieceIDs[0] != 1 || first.PieceIDs[1] != 3 {
		t.Fatalf("unexpected first incremental batch: %#v", first.PieceIDs)
	}

	tor.enqueueNewPiece(8)
	if err := peer.updateHavePieces(context.Background()); err != nil {
		t.Fatalf("unexpected error on second incremental update: %v", err)
	}
	if len(rl.queries) != 2 {
		t.Fatalf("expected second incremental update to append one batch, got %d", len(rl.queries))
	}
	second, ok := rl.queries[1].Update.(UpdateHavePieces)
	if !ok {
		t.Fatalf("expected UpdateHavePieces, got %T", rl.queries[1].Update)
	}
	if len(second.PieceIDs) != 1 || second.PieceIDs[0] != 8 {
		t.Fatalf("unexpected second incremental batch: %#v", second.PieceIDs)
	}
}

func TestStoragePeerPrepareTorrentInfo_UsesDedicatedInitControlQueue(t *testing.T) {
	info := &TorrentInfo{
		PieceSize:  4096,
		FileSize:   4096,
		RootHash:   make([]byte, 32),
		HeaderSize: 1,
		HeaderHash: make([]byte, 32),
	}
	infoCell, err := tlb.ToCell(info)
	if err != nil {
		t.Fatalf("failed to build torrent info cell: %v", err)
	}

	rl := &testRLDP{
		onQuery: func(query tl.Serializable, result tl.Serializable) error {
			req, _ := overlay.UnwrapQuery(query)
			switch req.(type) {
			case GetTorrentInfo, *GetTorrentInfo:
				res, ok := result.(*TorrentInfoContainer)
				if !ok {
					return errors.New("unexpected torrent info result type")
				}
				*res = TorrentInfoContainer{Data: infoCell.ToBOC()}
				return nil
			default:
				return errors.New("unexpected query type")
			}
		},
	}

	controlQueue := make(chan struct{}, 1)
	controlQueue <- struct{}{}

	tor := &Torrent{
		BagID:     infoCell.Hash(),
		globalCtx: context.Background(),
		pieceMask: []byte{0x00},
	}
	peer := &storagePeer{
		torrent: tor,
		conn: &PeerConnection{
			rldp:             rl,
			controlQueue:     controlQueue,
			initControlQueue: make(chan struct{}, 1),
		},
		overlay:  []byte("overlay"),
		nodeId:   []byte("node"),
		nodeAddr: "127.0.0.1:1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := peer.prepareTorrentInfo(ctx); err != nil {
		t.Fatalf("expected torrent info init to succeed while regular control queue is busy: %v", err)
	}
	if tor.Info == nil {
		t.Fatal("expected torrent info to be populated")
	}
	if tor.Info.PieceSize != info.PieceSize || tor.Info.HeaderSize != info.HeaderSize {
		t.Fatal("expected fetched torrent info to match remote metadata")
	}
}

func TestStoragePeerPendingInitChunks_FlushAfterInfoReady(t *testing.T) {
	peer := &storagePeer{}

	if err := peer.queuePendingInitChunk(0, []byte{0x01}); err != nil {
		t.Fatalf("unexpected queue error: %v", err)
	}
	if peer.hasPiece(0) {
		t.Fatal("expected queued init chunk to stay unapplied before info is ready")
	}

	complete, err := peer.flushPendingPieceUpdates(1)
	if err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if !complete {
		t.Fatal("expected queued init chunk to complete init after info is ready")
	}
	if !peer.hasPiece(0) {
		t.Fatal("expected queued init chunk to populate piece bitset after flush")
	}
	if len(peer.pendingInitChunks) != 0 || peer.pendingInitBytes != 0 {
		t.Fatal("expected pending init queue to be drained after flush")
	}
}

func TestStoragePeerPendingHavePieces_FlushAfterInfoReady(t *testing.T) {
	peer := &storagePeer{}

	if err := peer.queuePendingHavePieces([]int32{1, 3}); err != nil {
		t.Fatalf("unexpected queue error: %v", err)
	}
	if peer.hasPiece(1) || peer.hasPiece(3) {
		t.Fatal("expected queued have-pieces update to stay unapplied before info is ready")
	}

	complete, err := peer.flushPendingPieceUpdates(8)
	if err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}
	if complete {
		t.Fatal("did not expect have-pieces flush alone to mark init complete")
	}
	if !peer.hasPiece(1) || !peer.hasPiece(3) {
		t.Fatal("expected queued have-pieces update to populate bitset after flush")
	}
	if len(peer.pendingHavePieces) != 0 {
		t.Fatal("expected pending have-pieces queue to be drained after flush")
	}
}

func TestStoragePeerIsDownloadUsable_WithKnownPiecesBeforeFullInit(t *testing.T) {
	peer := &storagePeer{}
	atomic.StoreInt32(&peer.sessionInitialized, 1)
	atomic.StoreInt32(&peer.updateInitReceived, 0)
	atomic.StoreUint32(&peer.knownPieces, 3)

	if !peer.isDownloadUsable() {
		t.Fatal("expected peer with known pieces to be usable before full init completes")
	}
}

func TestStoragePeerIsDownloadUsable_RequiresSessionOrPieces(t *testing.T) {
	peer := &storagePeer{}

	if peer.isDownloadUsable() {
		t.Fatal("expected empty peer to be unusable")
	}

	atomic.StoreInt32(&peer.sessionInitialized, 1)
	if peer.isDownloadUsable() {
		t.Fatal("expected initialized peer without piece knowledge to stay unusable until init data arrives")
	}

	atomic.StoreInt32(&peer.updateInitReceived, 1)
	if !peer.isDownloadUsable() {
		t.Fatal("expected fully initialized peer to be usable")
	}
}

func TestStoragePeerPingWithRetry_RetriesTransientFailures(t *testing.T) {
	adnlPeer := &testADNLPeer{
		queryErrs: []error{
			errors.New("transient ping timeout"),
			errors.New("transient ping timeout"),
		},
	}
	peer := &storagePeer{
		torrent:   &Torrent{BagID: []byte("bag")},
		conn:      &PeerConnection{adnl: adnlPeer},
		nodeId:    []byte("node"),
		nodeAddr:  "127.0.0.1:1",
		sessionId: 77,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := peer.pingWithRetry(ctx); err != nil {
		t.Fatalf("expected ping retry to recover from transient failures: %v", err)
	}
	if got := adnlPeer.queryCalls.Load(); got != sessionPingAttempts {
		t.Fatalf("expected %d ping attempts, got %d", sessionPingAttempts, got)
	}
}

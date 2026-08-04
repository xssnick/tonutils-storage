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
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() {},
		session:     peerSessionState{sessionId: 111},
	}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreInt32(&peer.session.remoteInitComplete, 1)
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionAttempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected healthy session to be reused")
	}
	if sessionAttempt != nil {
		t.Fatal("expected healthy session reuse without reinitialization")
	}
	if atomic.LoadInt64(&peer.session.sessionId) != 111 {
		t.Fatal("expected session id to stay intact")
	}
}

func TestTorrentRouteIncomingSessionUpdateRejectsStaleSeqno(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	peer := &storagePeer{
		torrent: tor,
		nodeId:  []byte("node"),
		session: peerSessionState{sessionId: 77},
	}
	adnlID := []byte("adnl")

	if err := tor.routeIncomingSessionUpdate(peer, adnlID, incomingSessionUpdate{
		sessionID: 77,
		seqno:     1,
		update:    UpdateState{State: State{WillUpload: true, WantDownload: true}},
	}); err != nil {
		t.Fatalf("unexpected first update error: %v", err)
	}

	if err := tor.routeIncomingSessionUpdate(peer, adnlID, incomingSessionUpdate{
		sessionID: 77,
		seqno:     1,
		update:    UpdateState{State: State{WillUpload: false, WantDownload: true}},
	}); err == nil {
		t.Fatal("expected duplicate seqno to be rejected")
	}

	if err := tor.routeIncomingSessionUpdate(peer, adnlID, incomingSessionUpdate{
		sessionID: 77,
		seqno:     0,
		update:    UpdateState{State: State{WillUpload: true, WantDownload: true}},
	}); err == nil {
		t.Fatal("expected zero seqno to be rejected")
	}

	if err := tor.routeIncomingSessionUpdate(peer, adnlID, incomingSessionUpdate{
		sessionID: 77,
		seqno:     2,
		update:    UpdateState{State: State{WillUpload: true, WantDownload: true}},
	}); err != nil {
		t.Fatalf("unexpected newer update error: %v", err)
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
		session: peerSessionState{sessionId: 222},
	}
	atomic.StoreInt32(&peer.session.localInitSent, 0)
	atomic.StoreInt32(&peer.session.remoteInitComplete, 0)
	atomic.StoreInt64(&peer.session.sessionInitAt, time.Now().Add(-2*time.Minute).UnixMilli())
	atomic.StoreInt64(&peer.session.lastInitChunkAt, time.Now().Add(-50*time.Second).UnixMilli())
	atomic.StoreUint64(&peer.lastSentNewPiecesPos, 33)
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionAttempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected existing peer to be reinitialized in place")
	}
	if sessionAttempt == nil {
		t.Fatal("expected unhealthy session to request reinitialization")
	}
	if atomic.LoadInt32(&stopCalls) != 1 {
		t.Fatal("expected previous session context to be stopped before reinit")
	}
	if atomic.LoadInt64(&peer.session.sessionId) == 222 {
		t.Fatal("expected a fresh session id for outgoing reinit")
	}
	if atomic.LoadInt32(&peer.session.localInitSent) != 0 {
		t.Fatal("expected localInitSent to be reset before reinit")
	}
	if atomic.LoadInt32(&peer.session.remoteInitComplete) != 0 {
		t.Fatal("expected remoteInitComplete to be reset before reinit")
	}
	if atomic.LoadUint64(&peer.session.sessionGen) != sessionAttempt.generation {
		t.Fatal("expected returned attempt generation to match peer generation")
	}
	if atomic.LoadInt64(&peer.session.lastInitChunkAt) != 0 {
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
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() {},
		session:     peerSessionState{sessionId: 333},
	}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreInt32(&peer.session.remoteInitComplete, 0)
	atomic.StoreUint32(&peer.knownPieces, 12)
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionAttempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected initialized partial session to be reused")
	}
	if sessionAttempt != nil {
		t.Fatal("expected partial session reuse without reinitialization")
	}
	if atomic.LoadInt64(&peer.session.sessionId) != 333 {
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
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() { atomic.AddInt32(&stopCalls, 1) },
		session:     peerSessionState{sessionId: 444},
	}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreInt32(&peer.session.remoteInitComplete, 0)
	atomic.StoreUint32(&peer.knownPieces, 0)
	atomic.StoreInt64(&peer.session.sessionInitAt, time.Now().Add(-2*time.Minute).UnixMilli())
	atomic.StoreInt64(&peer.session.lastInitChunkAt, time.Now().Add(-50*time.Second).UnixMilli())
	conn.usedByBags[string(tor.BagID)] = peer

	gotPeer, sessionAttempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, nil)
	if gotPeer != peer {
		t.Fatal("expected existing peer to be reinitialized in place")
	}
	if sessionAttempt == nil {
		t.Fatal("expected unusable timed out session to request reinitialization")
	}
	if atomic.LoadInt32(&stopCalls) != 1 {
		t.Fatal("expected previous session context to be stopped before reinit")
	}
	if atomic.LoadInt64(&peer.session.sessionId) == 444 {
		t.Fatal("expected a fresh session id for outgoing reinit")
	}
}

func TestPrepareStoragePeer_ExplicitPingSessionReinitializesHealthySession(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
		Info:      &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1},
	}
	conn := &PeerConnection{
		usedByBags: map[string]*storagePeer{},
	}

	stopCalls := int32(0)
	peer := &storagePeer{
		torrent:     tor,
		nodeId:      []byte("node"),
		conn:        conn,
		closerCtx:   context.Background(),
		stop:        func() {},
		stopSession: func() { atomic.AddInt32(&stopCalls, 1) },
		session:     peerSessionState{sessionId: 555, sessionGen: 7},
	}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreInt32(&peer.session.remoteInitComplete, 1)
	peer.setRemoteState(State{WillUpload: true, WantDownload: true})
	conn.usedByBags[string(tor.BagID)] = peer

	newID := int64(556)
	gotPeer, attempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, &newID)
	if gotPeer != peer {
		t.Fatal("expected existing peer to be reinitialized in place")
	}
	if attempt == nil {
		t.Fatal("expected explicit new session id to create a fresh attempt")
	}
	if attempt.id != newID {
		t.Fatalf("expected new session id %d, got %d", newID, attempt.id)
	}
	if attempt.generation != 8 {
		t.Fatalf("expected session generation 8, got %d", attempt.generation)
	}
	if atomic.LoadInt32(&stopCalls) != 1 {
		t.Fatal("expected previous session context to be stopped")
	}
	if atomic.LoadInt32(&peer.session.localInitSent) != 0 || atomic.LoadInt32(&peer.session.remoteInitComplete) != 0 {
		t.Fatal("expected reinit to reset local and remote init flags")
	}
	if atomic.LoadInt32(&peer.session.remoteStateKnown) != 0 {
		t.Fatal("expected reinit to clear remote state")
	}
}

func TestSelectIncomingSessionID_OldAddUpdateCompatibility(t *testing.T) {
	t.Run("new peer accepts AddUpdate session id before ping", func(t *testing.T) {
		conn := &PeerConnection{usedByBags: map[string]*storagePeer{}}
		updateID := int64(77)

		got := selectIncomingSessionID(conn, []byte("bag"), &updateID)
		if got == nil || *got != updateID {
			t.Fatalf("expected update session id %d, got %v", updateID, got)
		}
	})

	t.Run("healthy peer keeps current session on mismatched AddUpdate", func(t *testing.T) {
		bagID := []byte("bag")
		conn := &PeerConnection{usedByBags: map[string]*storagePeer{}}
		peer := &storagePeer{
			session: peerSessionState{sessionId: 11},
		}
		atomic.StoreInt32(&peer.session.localInitSent, 1)
		atomic.StoreInt32(&peer.session.remoteInitComplete, 1)
		conn.usedByBags[string(bagID)] = peer

		updateID := int64(22)
		got := selectIncomingSessionID(conn, bagID, &updateID)
		if got == nil || *got != 11 {
			t.Fatalf("expected current healthy session id 11, got %v", got)
		}
	})

	t.Run("timed out unusable peer accepts AddUpdate reinit id", func(t *testing.T) {
		bagID := []byte("bag")
		conn := &PeerConnection{usedByBags: map[string]*storagePeer{}}
		peer := &storagePeer{
			session: peerSessionState{sessionId: 33},
		}
		atomic.StoreInt32(&peer.session.localInitSent, 1)
		atomic.StoreInt64(&peer.session.sessionInitAt, time.Now().Add(-2*time.Minute).UnixMilli())
		atomic.StoreInt64(&peer.session.lastInitChunkAt, time.Now().Add(-50*time.Second).UnixMilli())
		conn.usedByBags[string(bagID)] = peer

		updateID := int64(44)
		got := selectIncomingSessionID(conn, bagID, &updateID)
		if got == nil || *got != updateID {
			t.Fatalf("expected update session id %d for timed-out unusable peer, got %v", updateID, got)
		}
	})
}

func TestRouteIncomingPeerEvent_DefersOutboundInitUntilScheduled(t *testing.T) {
	initQuery := make(chan struct{}, 1)
	rl := &testRLDP{
		onQuery: func(query tl.Serializable, result tl.Serializable) error {
			select {
			case initQuery <- struct{}{}:
			default:
			}
			return nil
		},
	}
	tor := &Torrent{
		BagID:        []byte("bag"),
		Info:         &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1},
		globalCtx:    context.Background(),
		activeUpload: true,
		pieceMask:    []byte{0x01},
		peers:        map[string]*PeerInfo{},
		wake:         newWakeSig(),
	}
	conn := &PeerConnection{
		adnl:             &testADNLPeer{},
		rldp:             rl,
		usedByBags:       map[string]*storagePeer{},
		controlQueue:     make(chan struct{}, 1),
		initControlQueue: make(chan struct{}, 1),
	}
	updateSessionID := int64(91)

	peer, initTask := tor.routeIncomingPeerEvent(incomingPeerEvent{
		overlay:         []byte("overlay"),
		conn:            conn,
		updateSessionID: &updateSessionID,
	})
	if peer == nil {
		t.Fatal("expected incoming event to create a peer")
	}
	if initTask.attempt == nil {
		t.Fatal("expected incoming event to prepare outbound init")
	}
	if initTask.doPing {
		t.Fatal("expected AddUpdate-before-Ping compatibility path to skip outbound ping")
	}

	select {
	case <-initQuery:
		t.Fatal("outbound init should not run while handler is still routing the event")
	default:
	}

	if err := tor.routeIncomingSessionUpdate(peer, []byte("adnl"), incomingSessionUpdate{
		sessionID: updateSessionID,
		seqno:     1,
		update:    UpdateHavePieces{PieceIDs: []int32{0}},
	}); err != nil {
		t.Fatalf("unexpected incoming update error: %v", err)
	}

	select {
	case <-initQuery:
		t.Fatal("outbound init should stay deferred until the session task is scheduled")
	default:
	}

	initTask.schedule()
	select {
	case <-initQuery:
	case <-time.After(time.Second):
		t.Fatal("expected scheduled session task to send outbound init")
	}
}

func TestSessionInitTaskSchedule_DeduplicatesSameGeneration(t *testing.T) {
	initStarted := make(chan struct{}, 2)
	releaseInit := make(chan struct{})
	rl := &testRLDP{
		onQuery: func(query tl.Serializable, result tl.Serializable) error {
			select {
			case initStarted <- struct{}{}:
			default:
			}
			<-releaseInit
			return nil
		},
	}
	defer close(releaseInit)

	tor := &Torrent{
		BagID:        []byte("bag"),
		Info:         &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1},
		globalCtx:    context.Background(),
		activeUpload: true,
		pieceMask:    []byte{0x01},
		wake:         newWakeSig(),
	}
	peer := &storagePeer{
		torrent:   tor,
		conn:      &PeerConnection{adnl: &testADNLPeer{}, rldp: rl, controlQueue: make(chan struct{}, 1), initControlQueue: make(chan struct{}, 1)},
		overlay:   []byte("overlay"),
		nodeId:    []byte("node"),
		closerCtx: context.Background(),
		stop:      func() {},
		session:   peerSessionState{sessionId: 77, sessionGen: 1},
	}
	task := sessionInitTask{
		peer:    peer,
		attempt: newPeerSessionAttempt(context.Background(), 77, 1),
	}

	task.schedule()
	task.schedule()

	select {
	case <-initStarted:
	case <-time.After(time.Second):
		t.Fatal("expected first scheduled init to start")
	}

	select {
	case <-initStarted:
		t.Fatal("expected duplicate schedule for same generation to be ignored")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSessionInitTaskSchedule_AllowsNewGenerationAfterReinit(t *testing.T) {
	initSessions := make(chan int64, 2)
	rl := &testRLDP{
		onQuery: func(query tl.Serializable, result tl.Serializable) error {
			req, _ := overlay.UnwrapQuery(query)
			up, ok := req.(AddUpdate)
			if !ok {
				t.Fatalf("expected AddUpdate init query, got %T", req)
			}
			initSessions <- up.SessionID
			return nil
		},
	}

	tor := &Torrent{
		BagID:        []byte("bag"),
		Info:         &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1},
		globalCtx:    context.Background(),
		activeUpload: true,
		pieceMask:    []byte{0x01},
		wake:         newWakeSig(),
	}
	conn := &PeerConnection{
		adnl:             &testADNLPeer{},
		rldp:             rl,
		usedByBags:       map[string]*storagePeer{},
		controlQueue:     make(chan struct{}, 1),
		initControlQueue: make(chan struct{}, 1),
	}

	firstID := int64(77)
	peer, firstAttempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, &firstID)
	if firstAttempt == nil {
		t.Fatal("expected first session attempt")
	}
	sessionInitTask{peer: peer, attempt: firstAttempt}.schedule()

	select {
	case got := <-initSessions:
		if got != firstID {
			t.Fatalf("expected first init for session %d, got %d", firstID, got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected first init to be sent")
	}

	secondID := int64(78)
	_, secondAttempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, &secondID)
	if secondAttempt == nil {
		t.Fatal("expected reinit to create a new session attempt")
	}
	sessionInitTask{peer: peer, attempt: secondAttempt}.schedule()

	select {
	case got := <-initSessions:
		if got != secondID {
			t.Fatalf("expected reinit for session %d, got %d", secondID, got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected reinit generation to be sent")
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

	atomic.StoreInt64(&peer.session.sessionInitAt, now.Add(-2*time.Minute).UnixMilli())
	atomic.StoreInt64(&peer.session.lastInitChunkAt, now.Add(-10*time.Second).UnixMilli())
	if peer.initProgressTimedOut(now, 45*time.Second) {
		t.Fatal("expected recent init chunk to extend session init timeout")
	}

	atomic.StoreInt64(&peer.session.lastInitChunkAt, now.Add(-50*time.Second).UnixMilli())
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

func (t *testRLDP) Stats() rldp.Stats {
	return rldp.Stats{}
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
	}
	return nil
}

func (t *testRLDP) DoQueryAsync(_ context.Context, _ uint64, _ []byte, _ tl.Serializable, _ chan<- rldp.AsyncQueryResult) error {
	return nil
}

func (t *testRLDP) SetOnQuery(_ func([]byte, *rldp.Query) error) {}

func (t *testRLDP) SetOnMessage(_ func([]byte, []byte) error) {}

func (t *testRLDP) SetOnDisconnect(_ func()) {}

func (t *testRLDP) SendAnswer(_ context.Context, _ uint64, _ uint32, _, _ []byte, _ tl.Serializable) error {
	return nil
}

func TestPeerConnectionWithQueueSlot_ReleasesSlotOnError(t *testing.T) {
	conn := &PeerConnection{controlQueue: make(chan struct{}, 1)}
	expected := errors.New("boom")

	err := conn.withControlQueueSlot(context.Background(), true, func() error {
		if len(conn.controlQueue) != 1 {
			t.Fatal("expected control lane to be occupied while callback runs")
		}
		return expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected callback error, got %v", err)
	}

	select {
	case conn.controlQueue <- struct{}{}:
		<-conn.controlQueue
	default:
		t.Fatal("expected control lane slot to be released after callback error")
	}
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

func (t *testADNLPeer) SendNop(context.Context) error {
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

func (t *testADNLPeer) Stats() adnl.PeerStats {
	return adnl.PeerStats{}
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
		torrent: tor,
		conn:    &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 2)},
		overlay: []byte("overlay"),
		session: peerSessionState{sessionId: 77},
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
		torrent: tor,
		conn:    &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 2)},
		overlay: []byte("overlay"),
		session: peerSessionState{sessionId: 88},
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
		overlay: []byte("overlay"),
		session: peerSessionState{sessionId: 111},
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
		torrent: tor,
		conn:    &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 2)},
		overlay: []byte("overlay"),
		nodeId:  []byte("node"),
		session: peerSessionState{sessionId: 99},
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

func TestStoragePeerUpdateState_AdvertisesCurrentState(t *testing.T) {
	rl := &testRLDP{}
	tor := &Torrent{
		BagID:        []byte("bag"),
		globalCtx:    context.Background(),
		activeUpload: false,
	}
	peer := &storagePeer{
		torrent: tor,
		conn:    &PeerConnection{rldp: rl, controlQueue: make(chan struct{}, 1)},
		overlay: []byte("overlay"),
		session: peerSessionState{sessionId: 77},
	}

	if err := peer.updateState(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rl.queries) != 1 {
		t.Fatalf("expected one state update, got %d", len(rl.queries))
	}
	update, ok := rl.queries[0].Update.(UpdateState)
	if !ok {
		t.Fatalf("expected UpdateState, got %T", rl.queries[0].Update)
	}
	if update.State.WillUpload {
		t.Fatal("expected WillUpload=false when local upload is disabled")
	}
	if !update.State.WantDownload {
		t.Fatal("expected WantDownload=true while torrent is active")
	}
}

func TestStoragePeerInitializeSessionIgnoresStaleAttemptAfterNetwork(t *testing.T) {
	var peer *storagePeer
	rl := &testRLDP{
		onQuery: func(query tl.Serializable, result tl.Serializable) error {
			atomic.StoreUint64(&peer.session.sessionGen, 2)
			return nil
		},
	}
	tor := &Torrent{
		BagID:     []byte("bag"),
		Info:      &TorrentInfo{PieceSize: 4096, FileSize: 4096, HeaderSize: 1},
		globalCtx: context.Background(),
		pieceMask: []byte{0x00},
		wake:      newWakeSig(),
	}
	closed := int32(0)
	peer = &storagePeer{
		torrent:   tor,
		conn:      &PeerConnection{rldp: rl, initControlQueue: make(chan struct{}, 1), controlQueue: make(chan struct{}, 1)},
		overlay:   []byte("overlay"),
		nodeId:    []byte("node"),
		closerCtx: context.Background(),
		session:   peerSessionState{sessionId: 77, sessionGen: 1},
		stop: func() {
			atomic.AddInt32(&closed, 1)
		},
	}
	attempt := newPeerSessionAttempt(context.Background(), 77, 1)

	if err := peer.initializeSession(attempt, false); err != nil {
		t.Fatalf("stale attempt after network should finish without surfacing an error: %v", err)
	}
	if atomic.LoadInt32(&peer.session.localInitSent) != 0 {
		t.Fatal("stale attempt must not mark local init as sent")
	}
	if atomic.LoadInt32(&closed) != 0 {
		t.Fatal("stale attempt must not close the current peer")
	}
	if len(rl.queries) != 1 {
		t.Fatalf("expected one init query before attempt became stale, got %d", len(rl.queries))
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

func TestStoragePeerApplySessionUpdate_QueuesInitBeforeInfo(t *testing.T) {
	tor := &Torrent{
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	peer := &storagePeer{torrent: tor}

	err := peer.applySessionUpdate(UpdateInit{
		HavePieces:       []byte{0x01},
		HavePiecesOffset: 0,
		State: State{
			WillUpload:   true,
			WantDownload: true,
		},
	})
	if err != nil {
		t.Fatalf("unexpected pending init error: %v", err)
	}
	if peer.hasPiece(0) {
		t.Fatal("expected init chunk to stay queued until torrent info is available")
	}
	if atomic.LoadInt32(&peer.session.remoteStateKnown) != 1 || atomic.LoadInt32(&peer.session.remoteWillUpload) != 1 {
		t.Fatal("expected UpdateInit state to be recorded even before info is available")
	}

	tor.Info = &TorrentInfo{PieceSize: 1, FileSize: 1, HeaderSize: 1}
	if err = peer.applySessionUpdate(UpdateHavePieces{}); err != nil {
		t.Fatalf("unexpected flush trigger error: %v", err)
	}
	if !peer.hasPiece(0) {
		t.Fatal("expected queued init chunk to be applied after info becomes available")
	}
	if atomic.LoadInt32(&peer.session.remoteInitComplete) != 1 {
		t.Fatal("expected queued init chunk to mark remote init complete")
	}
}

func TestStoragePeerApplySessionUpdate_LegacyHavePiecesBeforeInfoAndInit(t *testing.T) {
	tor := &Torrent{
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	peer := &storagePeer{torrent: tor}
	atomic.StoreInt32(&peer.session.localInitSent, 1)

	if err := peer.applySessionUpdate(UpdateHavePieces{PieceIDs: []int32{2, 4}}); err != nil {
		t.Fatalf("unexpected pending have-pieces error: %v", err)
	}
	if peer.hasPiece(2) || peer.hasPiece(4) {
		t.Fatal("expected have-pieces update to stay queued until torrent info is available")
	}
	if peer.isDownloadUsable() {
		t.Fatal("expected peer to stay unusable before queued pieces can be applied")
	}

	tor.Info = &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1}
	if err := peer.applySessionUpdate(UpdateInit{
		HavePieces:       []byte{0x01},
		HavePiecesOffset: 0,
		State: State{
			WillUpload:   true,
			WantDownload: true,
		},
	}); err != nil {
		t.Fatalf("unexpected init-after-have error: %v", err)
	}

	if !peer.hasPiece(0) || !peer.hasPiece(2) || !peer.hasPiece(4) {
		t.Fatal("expected queued have-pieces and later init chunk to be applied")
	}
	if atomic.LoadInt32(&peer.session.remoteInitComplete) != 1 {
		t.Fatal("expected later init chunk to mark remote init complete")
	}
	if !peer.isDownloadUsable() {
		t.Fatal("expected peer to become usable after legacy reordered updates")
	}
}

func TestStoragePeerApplySessionUpdate_LegacyInitChunksOutOfOrderAndRetry(t *testing.T) {
	piecesNum := uint32(maxPiecesBytesPerRequest*8 + 1)
	tor := &Torrent{
		Info:      &TorrentInfo{PieceSize: 1, FileSize: uint64(piecesNum), HeaderSize: 1},
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	peer := &storagePeer{torrent: tor}

	tail := UpdateInit{
		HavePieces:       []byte{0x01},
		HavePiecesOffset: int32(maxPiecesBytesPerRequest * 8),
		State:            State{WillUpload: true, WantDownload: true},
	}
	if err := peer.applySessionUpdate(tail); err != nil {
		t.Fatalf("unexpected tail init chunk error: %v", err)
	}
	if atomic.LoadInt32(&peer.session.remoteInitComplete) != 0 {
		t.Fatal("expected init to stay incomplete until the missing first chunk arrives")
	}
	if !peer.hasPiece(piecesNum - 1) {
		t.Fatal("expected tail chunk to be applied before first chunk")
	}
	if peer.hasPiece(0) {
		t.Fatal("did not expect first piece before first chunk arrives")
	}

	if err := peer.applySessionUpdate(tail); err != nil {
		t.Fatalf("expected duplicate retry of the same init chunk to be accepted: %v", err)
	}

	firstMask := make([]byte, maxPiecesBytesPerRequest)
	firstMask[0] = 0x01
	if err := peer.applySessionUpdate(UpdateInit{
		HavePieces:       firstMask,
		HavePiecesOffset: 0,
		State:            State{WillUpload: true, WantDownload: true},
	}); err != nil {
		t.Fatalf("unexpected first init chunk error: %v", err)
	}
	if atomic.LoadInt32(&peer.session.remoteInitComplete) != 1 {
		t.Fatal("expected out-of-order chunks to complete remote init")
	}
	if !peer.hasPiece(0) || !peer.hasPiece(piecesNum-1) {
		t.Fatal("expected first and tail pieces to be tracked")
	}

	conflictingTail := tail
	conflictingTail.HavePieces = []byte{0x00}
	if err := peer.applySessionUpdate(conflictingTail); err != nil {
		t.Fatalf("expected stale duplicate init chunk to be accepted: %v", err)
	}
	if !peer.hasPiece(piecesNum - 1) {
		t.Fatal("expected stale duplicate init chunk to preserve already known pieces")
	}
}

func TestStoragePeerApplySessionUpdate_LegacyStateBeforeInit(t *testing.T) {
	tor := &Torrent{
		Info:      &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1},
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	peer := &storagePeer{torrent: tor}
	atomic.StoreInt32(&peer.session.localInitSent, 1)

	if err := peer.applySessionUpdate(UpdateState{State: State{WillUpload: false, WantDownload: true}}); err != nil {
		t.Fatalf("unexpected pre-init state error: %v", err)
	}
	if err := peer.applySessionUpdate(UpdateHavePieces{PieceIDs: []int32{3}}); err != nil {
		t.Fatalf("unexpected have-pieces error: %v", err)
	}
	if !peer.hasPiece(3) {
		t.Fatal("expected have-pieces to apply with info ready")
	}
	if peer.isDownloadUsable() {
		t.Fatal("expected WillUpload=false sent before init to block download usability")
	}

	if err := peer.applySessionUpdate(UpdateInit{
		HavePieces:       []byte{0x01},
		HavePiecesOffset: 0,
		State:            State{WillUpload: true, WantDownload: true},
	}); err != nil {
		t.Fatalf("unexpected init after state error: %v", err)
	}
	if !peer.isDownloadUsable() {
		t.Fatal("expected later UpdateInit state with WillUpload=true to restore usability")
	}
}

func TestPrepareStoragePeer_LegacyAddUpdateBeforePingThenUpdatesReordered(t *testing.T) {
	tor := &Torrent{
		BagID:     []byte("bag"),
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	conn := &PeerConnection{
		adnl:       &testADNLPeer{},
		usedByBags: map[string]*storagePeer{},
	}

	updateSessionID := int64(991)
	peer, attempt := tor.prepareStoragePeer([]byte("overlay"), nil, conn, &updateSessionID)
	if attempt == nil {
		t.Fatal("expected AddUpdate-before-Ping session id to create a local init attempt")
	}
	if attempt.id != updateSessionID {
		t.Fatalf("expected attempt id %d, got %d", updateSessionID, attempt.id)
	}

	atomic.StoreInt32(&peer.session.localInitSent, 1)
	if err := peer.applySessionUpdate(UpdateHavePieces{PieceIDs: []int32{5}}); err != nil {
		t.Fatalf("unexpected pending have-pieces error: %v", err)
	}
	tor.Info = &TorrentInfo{PieceSize: 1, FileSize: 8, HeaderSize: 1}
	if err := peer.applySessionUpdate(UpdateInit{
		HavePieces:       []byte{0x01},
		HavePiecesOffset: 0,
		State:            State{WillUpload: true, WantDownload: true},
	}); err != nil {
		t.Fatalf("unexpected init error after AddUpdate-before-Ping: %v", err)
	}
	if !peer.hasPiece(0) || !peer.hasPiece(5) {
		t.Fatal("expected reordered legacy updates to be applied after info arrives")
	}
	if !peer.isDownloadUsable() {
		t.Fatal("expected peer to be usable after AddUpdate-before-Ping compatibility flow")
	}
}

func TestStoragePeerApplySessionUpdate_AppliesUpdateState(t *testing.T) {
	tor := &Torrent{
		globalCtx: context.Background(),
		wake:      newWakeSig(),
	}
	peer := &storagePeer{torrent: tor}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreUint32(&peer.knownPieces, 1)

	if err := peer.applySessionUpdate(UpdateState{State: State{WillUpload: false, WantDownload: true}}); err != nil {
		t.Fatalf("unexpected update state error: %v", err)
	}
	if atomic.LoadInt32(&peer.session.remoteStateKnown) != 1 {
		t.Fatal("expected remote state to be marked known")
	}
	if atomic.LoadInt32(&peer.session.remoteWillUpload) != 0 || atomic.LoadInt32(&peer.session.remoteWantDownload) != 1 {
		t.Fatal("expected remote state flags to be stored")
	}
	if peer.isDownloadUsable() {
		t.Fatal("expected WillUpload=false state to make peer unusable for downloads")
	}
}

func TestStoragePeerIsDownloadUsable_WithKnownPiecesBeforeFullInit(t *testing.T) {
	peer := &storagePeer{}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreInt32(&peer.session.remoteInitComplete, 0)
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

	atomic.StoreInt32(&peer.session.localInitSent, 1)
	if peer.isDownloadUsable() {
		t.Fatal("expected initialized peer without piece knowledge to stay unusable until init data arrives")
	}

	atomic.StoreInt32(&peer.session.remoteInitComplete, 1)
	if !peer.isDownloadUsable() {
		t.Fatal("expected fully initialized peer to be usable")
	}
}

func TestStoragePeerIsDownloadUsable_HonorsRemoteWillUpload(t *testing.T) {
	peer := &storagePeer{}
	atomic.StoreInt32(&peer.session.localInitSent, 1)
	atomic.StoreUint32(&peer.knownPieces, 1)

	if !peer.isDownloadUsable() {
		t.Fatal("expected unknown remote state to preserve legacy partial-init usability")
	}

	peer.setRemoteState(State{WillUpload: false, WantDownload: true})
	if peer.isDownloadUsable() {
		t.Fatal("expected peer to be unusable when remote advertises WillUpload=false")
	}

	peer.setRemoteState(State{WillUpload: true, WantDownload: true})
	if !peer.isDownloadUsable() {
		t.Fatal("expected peer to become usable again when remote advertises WillUpload=true")
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
		torrent:  &Torrent{BagID: []byte("bag")},
		conn:     &PeerConnection{adnl: adnlPeer},
		nodeId:   []byte("node"),
		nodeAddr: "127.0.0.1:1",
		session:  peerSessionState{sessionId: 77},
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

package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type netCapacity struct {
}

type Server struct {
	key            ed25519.PrivateKey
	dht            DHT
	gate           *adnl.Gateway
	store          Storage
	closeCtx       context.Context
	serverMode     bool
	dhtParallelism int
	startedAt      time.Time

	netCtrl *rldp.TokenBucket

	bootstrapped map[string]*PeerConnection
	mx           sync.RWMutex

	dhtCache      map[string]*dhtCacheEntry
	dhtCacheMx    sync.RWMutex
	findSemaphore chan struct{}

	downloadMaxInflight atomic.Int64
	downloadInflight    atomic.Int64

	closer func()
}

type dhtCacheEntry struct {
	addrs    *address.List
	expireAt time.Time
}

func NewServer(dhtClient DHT, gate *adnl.Gateway, key ed25519.PrivateKey, serverMode bool, dhtParallelism int) *Server {
	var rateLimit = rldp.NewTokenBucket(1<<20, "storage")

	s := &Server{
		key:            key,
		dht:            dhtClient,
		gate:           gate,
		netCtrl:        rateLimit,
		bootstrapped:   map[string]*PeerConnection{},
		serverMode:     serverMode,
		dhtParallelism: dhtParallelism,
		startedAt:      time.Now(),
		dhtCache:       make(map[string]*dhtCacheEntry),
		findSemaphore:  make(chan struct{}, dhtParallelism),
	}
	s.downloadMaxInflight.Store(300) // TODO: high limit for now, add dynamic scaling based of bandwidth
	s.closeCtx, s.closer = context.WithCancel(context.Background())
	s.gate.SetConnectionHandler(s.bootstrapPeerWrap)

	go s.startPeerSearcher()

	if serverMode {
		go func() {
			wait := 1 * time.Second
			// refresh dht records
			for {
				select {
				case <-s.closeCtx.Done():
					Logger("[STORAGE_DHT] STOPPED DHT UPDATER")
					return
				case <-time.After(wait):
				}

				Logger("[STORAGE_DHT] UPDATING OUR ADDRESS RECORD...")

				ctx, cancel := context.WithTimeout(s.closeCtx, 180*time.Second)
				err := s.updateDHT(ctx)
				cancel()

				if err != nil {
					Logger("[STORAGE_DHT] FAILED TO UPDATE OUR ADDRESS RECORD", err.Error())

					// on err, retry sooner
					wait = 5 * time.Second
					continue
				}
				wait = 1 * time.Minute

				Logger("[STORAGE_DHT] OUR ADDRESS RECORD UPDATED")
			}
		}()
	}

	return s
}

func (s *Server) SetStorage(store Storage) {
	s.store = store
}

func (s *Server) bootstrapPeerWrap(client adnl.Peer) error {
	s.bootstrapPeer(client)
	return nil
}

func (s *Server) GetPeerIfActive(id []byte) *PeerConnection {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.bootstrapped[hex.EncodeToString(id)]
}

func (s *Server) bootstrapPeer(client adnl.Peer) *PeerConnection {
	s.mx.Lock()
	defer s.mx.Unlock()

	if rl := s.bootstrapped[hex.EncodeToString(client.GetID())]; rl != nil {
		return rl
	}

	extADNL := overlay.CreateExtendedADNL(client)
	extADNL.SetOnUnknownOverlayQuery(s.handleQuery(extADNL))

	rl := overlay.CreateExtendedRLDP(rldp.NewClientV2(extADNL))
	rl.SetOnUnknownOverlayQuery(s.handleRLDPQuery(rl))

	rl.SetOnDisconnect(func() {
		s.mx.Lock()
		delete(s.bootstrapped, hex.EncodeToString(client.GetID()))
		s.mx.Unlock()
	})

	p := &PeerConnection{
		rldp:             rl,
		adnl:             client,
		srv:              s,
		usedByBags:       map[string]*storagePeer{},
		controlQueue:     make(chan struct{}, 4),
		initControlQueue: make(chan struct{}, 2),
		dataQueue:        make(chan struct{}, DownloadPeerInflightCap),
		bagsInitQueue:    make(chan struct{}, 8),
	}
	p.MaxInflightPieces.Store(DownloadInitialPeerInflight)
	s.bootstrapped[hex.EncodeToString(client.GetID())] = p

	return p
}

func answerADNLStorageError(peer *overlay.ADNLWrapper, queryID []byte, err error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if sendErr := peer.Answer(ctx, queryID, StorageError{Message: err.Error()}); sendErr != nil {
		return fmt.Errorf("%w; failed to send storage error: %v", err, sendErr)
	}
	return nil
}

func answerRLDPStorageError(peer *overlay.RLDPWrapper, transfer []byte, query *rldp.Query, err error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if sendErr := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, StorageError{Message: err.Error()}); sendErr != nil {
		return fmt.Errorf("%w; failed to send storage error: %v", err, sendErr)
	}
	return nil
}

func (s *Server) handleQuery(peer *overlay.ADNLWrapper) func(query *adnl.MessageQuery) error {
	return func(query *adnl.MessageQuery) error {
		req, over := overlay.UnwrapQuery(query.Data)

		if s.store == nil {
			return answerADNLStorageError(peer, query.ID, fmt.Errorf("storage is not yet initialized"))
		}

		t := s.store.GetTorrentByOverlay(over)
		if t == nil {
			return answerADNLStorageError(peer, query.ID, fmt.Errorf("not found"))
		}

		isDow, isUpl := t.IsActive()
		if !isUpl && !isDow {
			return answerADNLStorageError(peer, query.ID, fmt.Errorf("bag %s is not active", hex.EncodeToString(t.BagID)))
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		switch q := req.(type) {
		case overlay.GetRandomPeers:
			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return answerADNLStorageError(peer, query.ID, err)
			}

			peers := []overlay.Node{*node}
			t.peersMx.RLock()
			for _, nd := range t.peers {
				if nd.peer.overlayNode == nil {
					continue
				}

				peers = append(peers, *nd.peer.overlayNode)
				if len(peers) == 8 {
					break
				}
			}
			t.peersMx.RUnlock()

			err = peer.Answer(ctx, query.ID, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case Ping:
			p := s.GetPeerIfActive(peer.GetID())
			if p == nil {
				return answerADNLStorageError(peer, query.ID, fmt.Errorf("peer disconnected"))
			}

			_, sessionInit := t.routeIncomingPeerEvent(incomingPeerEvent{
				overlay:   over,
				conn:      p,
				sessionID: &q.SessionID,
			})
			defer sessionInit.schedule()

			if err := peer.Answer(ctx, query.ID, Pong{}); err != nil {
				return err
			}
		default:
			return answerADNLStorageError(peer, query.ID, fmt.Errorf("unsupported storage query %T", req))
		}

		return nil
	}
}

func (s *Server) handleRLDPQuery(peer *overlay.RLDPWrapper) func(transfer []byte, query *rldp.Query) error {
	return func(transfer []byte, query *rldp.Query) error {
		req, over := overlay.UnwrapQuery(query.Data)

		if s.store == nil {
			return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("storage is not yet initialized"))
		}

		t := s.store.GetTorrentByOverlay(over)
		if t == nil {
			return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("not found"))
		}

		isDow, isUpl := t.IsActive()
		if !isDow && !isUpl {
			return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("bag %s is not active", hex.EncodeToString(t.BagID)))
		}

		adnlId := peer.GetADNL().GetID()

		var sesId *int64
		var updateSesId *int64
		var updateSessionID int64
		var timeout = 7 * time.Second

		switch q := req.(type) {
		case GetPiece:
			timeout = t.transmitTimeout()
		case Ping:
			sesId = &q.SessionID
		case AddUpdate:
			updateSessionID = q.SessionID
			updateSesId = &updateSessionID
			timeout = 20 * time.Second
		}

		p := s.GetPeerIfActive(adnlId)
		if p == nil {
			return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("peer disconnected"))
		}

		stPeer, sessionInit := t.routeIncomingPeerEvent(incomingPeerEvent{
			overlay:         over,
			conn:            p,
			sessionID:       sesId,
			updateSessionID: updateSesId,
		})
		defer sessionInit.schedule()

		ctx, cancel := context.WithTimeout(t.globalCtx, timeout)
		defer cancel()

		switch q := req.(type) {
		case overlay.GetRandomPeers:
			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return answerRLDPStorageError(peer, transfer, query, err)
			}

			peers := []overlay.Node{*node}
			t.peersMx.RLock()
			for _, nd := range t.peers {
				if nd.peer.overlayNode == nil {
					continue
				}

				peers = append(peers, *nd.peer.overlayNode)
				if len(peers) == 8 {
					break
				}
			}
			t.peersMx.RUnlock()

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case GetPiece:
			if !isUpl {
				return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("bag is not for upload"))
			}

			tm := time.Now()

			err := t.GetConnector().ThrottleUpload(ctx, uint64(t.Info.PieceSize))
			if err != nil {
				return answerRLDPStorageError(peer, transfer, query, err)
			}

			pc, err := t.GetPiece(uint32(q.PieceID))
			if err != nil {
				return answerRLDPStorageError(peer, transfer, query, err)
			}

			Logger("[STORAGE] LOADED PIECE", q.PieceID, hex.EncodeToString(adnlId), "TIME", time.Since(tm).String())

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, pc)
			if err != nil {
				Logger("[STORAGE] FAIL ANSWER PIECE", q.PieceID, hex.EncodeToString(adnlId), "TIME", time.Since(tm).String())

				return err
			}

			Logger("[STORAGE] SENT PIECE", q.PieceID, hex.EncodeToString(adnlId), "TIME", time.Since(tm).String())

			t.UpdateUploadedPeer(stPeer, uint64(len(pc.Data)))
		case Ping:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, Pong{})
			if err != nil {
				return err
			}
		case GetTorrentInfo:
			if !isUpl {
				return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("bag is not for upload"))
			}

			Logger("[STORAGE] SENDING TORRENT INFO TO", hex.EncodeToString(adnlId), "FOR", hex.EncodeToString(t.BagID))

			c, err := tlb.ToCell(t.Info)
			if err != nil {
				return answerRLDPStorageError(peer, transfer, query, err)
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, TorrentInfoContainer{
				Data: c.ToBOC(),
			})
			if err != nil {
				Logger("[STORAGE] FAILED TO SEND TORRENT INFO TO", hex.EncodeToString(adnlId), "FOR", hex.EncodeToString(t.BagID), err.Error())
				return err
			}
			Logger("[STORAGE] SENT TORRENT INFO TO", hex.EncodeToString(adnlId), "FOR", hex.EncodeToString(t.BagID))
		case AddUpdate:
			if err := t.routeIncomingSessionUpdate(stPeer, adnlId, incomingSessionUpdate{
				sessionID: q.SessionID,
				seqno:     q.Seqno,
				update:    q.Update,
			}); err != nil {
				return answerRLDPStorageError(peer, transfer, query, err)
			}

			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, &Ok{})
			if err != nil {
				return err
			}
		default:
			return answerRLDPStorageError(peer, transfer, query, fmt.Errorf("unsupported storage query %T", req))
		}

		return nil
	}
}

type incomingPeerEvent struct {
	overlay         []byte
	overlayNode     *overlay.Node
	conn            *PeerConnection
	sessionID       *int64
	updateSessionID *int64
}

type sessionInitTask struct {
	peer    *storagePeer
	attempt *peerSessionAttempt
	doPing  bool
}

func (t *Torrent) routeIncomingPeerEvent(event incomingPeerEvent) (*storagePeer, sessionInitTask) {
	sessionID := event.sessionID
	if sessionID == nil {
		sessionID = selectIncomingSessionID(event.conn, t.BagID, event.updateSessionID)
	}

	stPeer, sessionAttempt := t.prepareStoragePeer(event.overlay, event.overlayNode, event.conn, sessionID)
	stPeer.touch()
	if sessionAttempt == nil {
		return stPeer, sessionInitTask{}
	}

	return stPeer, sessionInitTask{
		peer:    stPeer,
		attempt: sessionAttempt,
		doPing:  sessionID == nil,
	}
}

func (t sessionInitTask) schedule() {
	if t.peer == nil || t.attempt == nil {
		return
	}
	if !t.peer.session.tryScheduleInit(*t.attempt) {
		return
	}

	go func() {
		_ = t.peer.initializeSession(t.attempt, t.doPing)
	}()
}

type incomingSessionUpdate struct {
	sessionID int64
	seqno     int64
	update    any
}

func (t *Torrent) routeIncomingSessionUpdate(peer *storagePeer, adnlID []byte, update incomingSessionUpdate) error {
	currentSessionID := atomic.LoadInt64(&peer.session.sessionId)
	if update.sessionID != currentSessionID {
		Logger("UPDATE SESSION MISSMATCH", update.sessionID, currentSessionID, hex.EncodeToString(adnlID), hex.EncodeToString(t.BagID))
		return fmt.Errorf("session id mismatch")
	}
	if !peer.session.acceptRemoteSeqno(update.seqno) {
		Logger("UPDATE SESSION STALE SEQNO", update.seqno, hex.EncodeToString(adnlID), hex.EncodeToString(t.BagID))
		return fmt.Errorf("stale session update seqno")
	}

	switch update.update.(type) {
	case UpdateInit:
		Logger("[STORAGE] NODE REPORTED INIT PIECES INFO", hex.EncodeToString(adnlID), update.sessionID, update.seqno)
	case UpdateHavePieces:
		Logger("[STORAGE] NODE HAS NEW PIECES", hex.EncodeToString(adnlID))
	}

	return peer.applySessionUpdate(update.update)
}

func selectIncomingSessionID(conn *PeerConnection, bagID []byte, updateSesId *int64) *int64 {
	if existing := conn.GetFor(bagID); existing != nil {
		current := atomic.LoadInt64(&existing.session.sessionId)
		if updateSesId != nil && (current == 0 || current == *updateSesId ||
			(!existing.isDownloadUsable() && existing.initProgressTimedOut(time.Now(), 45*time.Second))) {
			return updateSesId
		}
		if current != 0 {
			return &current
		}
		return nil
	}
	if updateSesId != nil {
		return updateSesId
	}
	return nil
}

const maxPiecesBytesPerRequest = 6000
const maxNewPiecesPerRequest = maxPiecesBytesPerRequest / 4
const peerSearcherInterval = time.Second

const (
	bagDHTRefreshAge = 3 * time.Minute
	// tonutils-go treats overlay DHT values as stale when every node version is older than this.
	bagDHTOverlayNodeFreshFor         = 10 * time.Minute
	bagDHTSteadyRefreshInterval       = 5 * time.Minute
	bagDHTSteadyRefreshJitter         = 30 * time.Second
	bagDHTSeedRefreshInterval         = 4 * time.Minute
	bagDHTSeedInitialSpreadWindow     = 4 * time.Minute
	bagDHTSeedRetryBaseInterval       = 15 * time.Second
	bagDHTSeedRetryMaxInterval        = time.Minute
	bagDHTSeedMinStoredNodes          = 3
	bagDHTActiveDownloadRetryInterval = 7 * time.Second
	bagDHTActiveDownloadPeerInterval  = 45 * time.Second
	bagDHTZeroPeerRetryInterval       = 15 * time.Second
	bagDHTZeroPeerMaxRetryInterval    = 7 * time.Minute
	bagDHTZeroPeerRetryJitter         = 5 * time.Second
)

func bagDHTNextAttemptAt(lastAttemptAt, lastCompletedAt int64, activeDownload bool, numPeers, usableDownloadPeers int, serverMode bool, zeroPeerSearches uint32, jitter time.Duration) time.Time {
	baseAt := time.Time{}
	if lastAttemptAt > 0 {
		baseAt = time.Unix(0, lastAttemptAt)
	}
	// Use completion time after queued work finishes so many bags stay spread across refresh cycles.
	if lastCompletedAt > lastAttemptAt {
		baseAt = time.Unix(0, lastCompletedAt)
	}

	return baseAt.Add(bagDHTAttemptDelay(activeDownload, numPeers, usableDownloadPeers, serverMode, zeroPeerSearches, jitter))
}

func bagDHTAttemptDelay(activeDownload bool, numPeers, usableDownloadPeers int, serverMode bool, zeroPeerSearches uint32, jitter time.Duration) time.Duration {
	if activeDownload {
		if usableDownloadPeers > 0 {
			return bagDHTActiveDownloadPeerInterval
		}
		return bagDHTActiveDownloadRetryInterval
	}

	if numPeers > 0 {
		return bagDHTSteadyRefreshInterval + clampDuration(jitter, bagDHTSteadyRefreshJitter)
	}

	if serverMode {
		return bagDHTSteadyRefreshInterval + clampDuration(jitter, bagDHTSteadyRefreshJitter)
	}

	delay := bagDHTZeroPeerRetryInterval * time.Duration(zeroPeerSearches+1)
	if delay > bagDHTZeroPeerMaxRetryInterval {
		delay = bagDHTZeroPeerMaxRetryInterval
	}
	return delay + clampDuration(jitter, bagDHTZeroPeerRetryJitter)
}

func bagDHTAttemptJitter(activeDownload bool, numPeers int, serverMode bool) time.Duration {
	if activeDownload {
		return 0
	}
	if numPeers > 0 || serverMode {
		return randomDuration(bagDHTSteadyRefreshJitter)
	}
	return randomDuration(bagDHTZeroPeerRetryJitter)
}

func bagDHTSeedNextAttemptAt(serverStartedAt time.Time, bagID []byte, lastCompletedAt, lastSuccessAt int64, failed bool, failStreak uint32) time.Time {
	if failed && lastCompletedAt > 0 {
		return time.Unix(0, lastCompletedAt).Add(bagDHTSeedRetryDelay(failStreak))
	}

	if lastSuccessAt > 0 {
		return time.Unix(0, lastSuccessAt).Add(bagDHTSeedRefreshInterval)
	}

	if serverStartedAt.IsZero() {
		return time.Time{}
	}
	return serverStartedAt.Add(bagDHTSeedSpreadOffset(bagID, bagDHTSeedInitialSpreadWindow))
}

func bagDHTSeedRetryDelay(failStreak uint32) time.Duration {
	if failStreak == 0 {
		failStreak = 1
	}

	delay := bagDHTSeedRetryBaseInterval
	for i := uint32(1); i < failStreak; i++ {
		delay *= 2
		if delay >= bagDHTSeedRetryMaxInterval {
			return bagDHTSeedRetryMaxInterval
		}
	}
	return delay
}

func bagDHTSeedSpreadOffset(bagID []byte, window time.Duration) time.Duration {
	if window <= 0 || len(bagID) == 0 {
		return 0
	}

	var hash uint64 = 1469598103934665603
	for _, b := range bagID {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return time.Duration(hash % uint64(window))
}

func bagDHTSeedLaunchBudget(seedCount, dhtParallelism int) int {
	if seedCount <= 0 || dhtParallelism <= 0 {
		return 0
	}

	periodSeconds := int(bagDHTSeedRefreshInterval / time.Second)
	if periodSeconds <= 0 {
		periodSeconds = 1
	}

	budget := (seedCount + periodSeconds - 1) / periodSeconds
	if budget < 1 {
		budget = 1
	}
	budget += (budget + 3) / 4
	if budget > dhtParallelism {
		return dhtParallelism
	}
	return budget
}

func randomDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}

func clampDuration(v, max time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return v
}

func (p *storagePeer) updateInitPieces(ctx context.Context) error {
	attempt := p.currentSessionAttempt(ctx)
	return p.sendInitPieces(ctx, attempt)
}

func (p *storagePeer) sendInitPieces(ctx context.Context, attempt peerSessionAttempt) error {
	if !p.isSessionAttemptCurrent(attempt) {
		return errStaleSessionAttempt
	}

	num := p.torrent.Info.PiecesNum()
	isDow, isUpl := p.torrent.IsActiveRaw()
	cursor := p.torrent.currentNewPiecesCursor()

	p.piecesMx.Lock()
	p.lastSentPieces = p.torrent.PiecesMask()
	atomic.StoreUint64(&p.lastSentNewPiecesPos, cursor)
	p.piecesMx.Unlock()

	sent := uint32(0)
	for i := uint32(0); sent < num; i++ {
		if !p.isSessionAttemptCurrent(attempt) {
			return errStaleSessionAttempt
		}

		p.piecesMx.RLock()
		have := p.lastSentPieces[i*maxPiecesBytesPerRequest:]
		p.piecesMx.RUnlock()

		if len(have) > maxPiecesBytesPerRequest {
			have = have[:maxPiecesBytesPerRequest]
		}

		up := AddUpdate{
			SessionID: attempt.id,
			Seqno:     atomic.AddInt64(&p.session.sessionSeqno, 1),
			Update: UpdateInit{
				HavePieces:       have,
				HavePiecesOffset: int32(sent),
				State: State{
					WillUpload:   isUpl,
					WantDownload: isDow,
				},
			},
		}

		var updRes Ok

		err := p.conn.withInitControlQueueSlot(ctx, true, func() error {
			ctxReq, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()

			return p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &updRes)
		})
		if err != nil {
			Logger("[STORAGE] FAILED TO SEND UPDATE INIT", i, hex.EncodeToString(p.conn.adnl.GetID()), attempt.id, err.Error())
			return fmt.Errorf("failed to send have pieces update: %w", err)
		}

		sent += uint32(len(have) * 8)
	}

	p.torrent.compactNewPieces()

	return nil
}

func (p *storagePeer) updateHavePieces(ctx context.Context) error {
	attempt := p.currentSessionAttempt(ctx)
	for {
		if !p.isSessionAttemptCurrent(attempt) {
			return nil
		}

		cursor := atomic.LoadUint64(&p.lastSentNewPiecesPos)
		have, nextCursor := p.torrent.getNewPiecesSince(cursor, maxNewPiecesPerRequest)
		if len(have) == 0 {
			return nil
		}

		up := AddUpdate{
			SessionID: attempt.id,
			Seqno:     atomic.AddInt64(&p.session.sessionSeqno, 1),
			Update: UpdateHavePieces{
				PieceIDs: have,
			},
		}

		var updRes Ok

		err := p.conn.withControlQueueSlot(ctx, false, func() error {
			ctxReq, cancel := context.WithTimeout(ctx, 7*time.Second)
			defer cancel()

			return p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &updRes)
		})
		if err != nil {
			Logger("[STORAGE] FAILED TO SEND UPDATE HAVE", hex.EncodeToString(p.conn.adnl.GetID()), attempt.id, err.Error())
			return fmt.Errorf("failed to send have pieces update: %w", err)
		}

		if !p.isSessionAttemptCurrent(attempt) {
			return nil
		}
		atomic.StoreUint64(&p.lastSentNewPiecesPos, nextCursor)
		p.torrent.compactNewPieces()
	}
}

func (p *storagePeer) updateState(ctx context.Context) error {
	attempt := p.currentSessionAttempt(ctx)
	if !p.isSessionAttemptCurrent(attempt) {
		return errStaleSessionAttempt
	}

	isDow, isUpl := p.torrent.IsActiveRaw()
	up := AddUpdate{
		SessionID: attempt.id,
		Seqno:     atomic.AddInt64(&p.session.sessionSeqno, 1),
		Update: UpdateState{
			State: State{
				WillUpload:   isUpl,
				WantDownload: isDow,
			},
		},
	}

	var res Ok
	err := p.conn.withControlQueueSlot(ctx, true, func() error {
		ctxReq, cancel := context.WithTimeout(ctx, 7*time.Second)
		defer cancel()

		return p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &res)
	})
	if err != nil {
		Logger("[STORAGE] FAILED TO SEND UPDATE STATE", hex.EncodeToString(p.conn.adnl.GetID()), err.Error())
		return fmt.Errorf("failed to send state update: %w", err)
	}
	return nil
}

func (s *Server) updateDHT(ctx context.Context) error {
	addr := s.gate.GetAddressList()

	ctxStore, cancel := context.WithTimeout(ctx, 90*time.Second)
	stored, id, err := s.dht.StoreAddress(ctxStore, addr, 20*time.Minute, s.key)
	cancel()
	if err != nil && stored == 0 {
		return err
	}

	// make sure it was saved
	_, _, err = s.dht.FindAddresses(ctx, id)
	if err != nil {
		return err
	}

	Logger("[STORAGE_DHT] OUR NODE ADDRESS UPDATED ON", stored, "NODES")

	return nil
}

func (s *Server) storeBagDHTSelf(ctx context.Context, torrent *Torrent) (int, error) {
	node, err := overlay.NewNode(torrent.BagID, s.key)
	if err != nil {
		Logger("[STORAGE_DHT] FAILED CREATE OVERLAY NODE FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return 0, err
	}

	nodesList := &overlay.NodesList{
		List: []overlay.Node{*node},
	}

	return s.storeBagDHTNodes(ctx, torrent, nodesList)
}

func (s *Server) storeBagDHTNodes(ctx context.Context, torrent *Torrent, nodesList *overlay.NodesList) (int, error) {
	Logger("[STORAGE] STORING BAG DHT RECORD", hex.EncodeToString(torrent.BagID))

	tm := time.Now()
	ctxStore, cancel := context.WithTimeout(ctx, 120*time.Second)
	stored, _, err := s.dht.StoreOverlayNodes(ctxStore, torrent.BagID, nodesList, 45*time.Minute)
	cancel()
	if err != nil && stored == 0 {
		Logger("[STORAGE_DHT] FAILED TO STORE DHT OVERLAY RECORD FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return stored, err
	}
	Logger("[STORAGE_DHT] BAG OVERLAY UPDATED ON", stored, "NODES FOR", hex.EncodeToString(torrent.BagID), "TOOK", time.Since(tm).String())
	if stored > 0 && stored < bagDHTSeedMinStoredNodes {
		Logger("[STORAGE_DHT] BAG OVERLAY LOW REPLICATION", stored, "NODES FOR", hex.EncodeToString(torrent.BagID))
	}
	return stored, nil
}

func (s *Server) checkAndUpdateBagDHT(ctx context.Context, torrent *Torrent, isServer bool) error {
	Logger("[STORAGE_DHT] CHECKING BAG OVERLAY FOR", hex.EncodeToString(torrent.BagID))

	tm := time.Now()
	nodesList, _, err := s.dht.FindOverlayNodes(ctx, torrent.BagID)
	if err != nil && !errors.Is(err, dht.ErrDHTValueIsNotFound) {
		Logger("[STORAGE_DHT] FAILED TO FIND DHT OVERLAY RECORD FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return err
	}

	if nodesList == nil {
		nodesList = &overlay.NodesList{}
	} else {
		for i := range nodesList.List {
			torrent.addNode(nodesList.List[i])
		}
	}

	Logger("[STORAGE_DHT] FOUND", len(nodesList.List), "OVERLAY NODES FOR", hex.EncodeToString(torrent.BagID), "TOOK", time.Since(tm).String())

	node, err := overlay.NewNode(torrent.BagID, s.key)
	if err != nil {
		Logger("[STORAGE_DHT] FAILED CREATE OVERLAY NODE FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return err
	}

	hasExpired := false
	refreshed := false
	tooEarly := false
	now := time.Now()
	refreshAfter := int32(now.Add(-bagDHTRefreshAge).Unix())
	expiredBefore := int32(now.Add(-bagDHTOverlayNodeFreshFor).Unix())
	// refresh if already exists
	for i := range nodesList.List {
		id, ok := nodesList.List[i].ID.(keys.PublicKeyED25519)
		if ok && id.Key.Equal(node.ID.(keys.PublicKeyED25519).Key) {
			if nodesList.List[i].Version > refreshAfter {
				tooEarly = true
			} else {
				nodesList.List[i] = *node
			}

			refreshed = true
			break
		}

		if nodesList.List[i].Version <= expiredBefore {
			hasExpired = true
		}
	}

	if !refreshed {
		// create if no records
		if len(nodesList.List) == 0 {
			nodesList = &overlay.NodesList{
				List: []overlay.Node{*node},
			}
			refreshed = true
		} else {
			if len(nodesList.List) >= 5 {
				// only allowed to replace when have public ip, or something expired in the list
				if isServer || hasExpired {
					sort.Slice(nodesList.List, func(i, j int) bool {
						return nodesList.List[i].Version < nodesList.List[j].Version
					})

					// replace oldest
					nodesList.List[0] = *node
					refreshed = true
				}
			} else {
				// add our node if < 5 in list
				nodesList.List = append(nodesList.List, *node)
				refreshed = true
			}
		}
	}

	if refreshed && !tooEarly {
		_, err = s.storeBagDHTNodes(ctx, torrent, nodesList)
		return err
	}

	return nil
}

func (s *Server) Stop() {
	s.closer()
	return
}

func (t *Torrent) addNode(node overlay.Node) {
	nodeId, err := tl.Hash(node.ID)
	if err != nil {
		return
	}

	strId := hex.EncodeToString(nodeId)

	if node.CheckSignature() != nil {
		Logger("[STORAGE] INCORRECT OVERLAY NODE SIGNATURE, SKIPPED", strId)
		return
	}

	if bytes.Equal(nodeId, t.connector.GetID()) {
		Logger("[STORAGE] SKIP OURSELF", strId)

		// skip ourself
		return
	}

	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	if p := t.peers[strId]; p != nil && (p.peer.overlayNode == nil || p.peer.overlayNode.Version < node.Version) {
		p.peer.overlayNode = &node
		Logger("[STORAGE] UPDATED PEER OVERLAY NODE INFO", strId)
	}

	if t.knownNodes[strId] == nil {
		if len(t.knownNodes) > 128 { // cleanup
			var evictKey string
			var evictNode *KnownNode
			for n, kn := range t.knownNodes {
				if evictNode == nil || evictNode.FailStreak.Load() < kn.FailStreak.Load() {
					evictNode = kn
					evictKey = n
					continue
				}
			}

			if evictNode != nil {
				Logger("[STORAGE] EVICTED KNOWN NODE ", evictKey, "FOR", hex.EncodeToString(t.BagID))

				delete(t.knownNodes, evictKey)
			}
		}

		Logger("[STORAGE] ADD KNOWN NODE ", strId, "FOR", hex.EncodeToString(t.BagID))
		n := &KnownNode{}
		n.Node.Store(&node)
		t.knownNodes[strId] = n
	} else if node.Version > t.knownNodes[strId].Node.Load().Version {
		t.knownNodes[strId].Node.Store(&node)
		Logger("[STORAGE] UPDATED KNOWN NODE", strId)
	}
}

func (s *Server) ConnectToNode(ctx context.Context, t *Torrent, node *overlay.Node, addrs *address.List) error {
	key, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return fmt.Errorf("invalid node type")
	}

	adnlID, err := tl.Hash(key)
	if err != nil {
		return fmt.Errorf("failed build adnl from node key: %w", err)
	}

	peer := s.GetPeerIfActive(adnlID)
	if peer == nil {
		start := time.Now()
		if addrs == nil {

			adnlIDStr := hex.EncodeToString(adnlID)

			s.dhtCacheMx.RLock()
			if entry := s.dhtCache[adnlIDStr]; entry != nil && entry.expireAt.After(time.Now()) {
				addrs = entry.addrs
				s.dhtCacheMx.RUnlock()
			} else {
				s.dhtCacheMx.RUnlock()

				select {
				case s.findSemaphore <- struct{}{}:
					defer func() { <-s.findSemaphore }()
				case <-ctx.Done():
					return ctx.Err()
				}

				lcCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
				addrs, _, err = s.dht.FindAddresses(lcCtx, adnlID)
				cancel()
				if err != nil {
					Logger("[STORAGE] NOT FOUND NODE ADDR OF", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID), "ERR", err.Error(), "TOOK", time.Since(start).String())
					return fmt.Errorf("failed to find node address: %w", err)
				}

				s.dhtCacheMx.Lock()
				s.dhtCache[adnlIDStr] = &dhtCacheEntry{
					addrs:    addrs,
					expireAt: time.Now().Add(150 * time.Second),
				}
				s.dhtCacheMx.Unlock()
			}
		}
		if addrs == nil || len(addrs.Addresses) == 0 {
			return fmt.Errorf("node has no known addresses")
		}
		addr, err := address.DialString(addrs.Addresses[0])
		if err != nil {
			return fmt.Errorf("failed to format node address: %w", err)
		}
		Logger("[STORAGE] ADDR FOR NODE ", hex.EncodeToString(adnlID), "FOUND", addr, "PUBKEY", hex.EncodeToString(key.Key), "FOR", hex.EncodeToString(t.BagID), "ELAPSED", time.Since(start).Seconds())

		ax, err := s.gate.RegisterClient(addr, key.Key)
		if err != nil {
			return fmt.Errorf("failed to connnect to node: %w", err)
		}
		peer = s.bootstrapPeer(ax)
	} else {
		Logger("[STORAGE] HAS ALREADY ACTIVE PEER FOR NODE ", hex.EncodeToString(adnlID), "ADDR", peer.adnl.RemoteAddr(), "ADDING FOR", hex.EncodeToString(t.BagID))
	}

	stNode, sessionAttempt := t.prepareStoragePeer(node.Overlay, node, peer, nil)

	select {
	case <-ctx.Done():
		stNode.Close()
		return ctx.Err()
	case peer.bagsInitQueue <- struct{}{}:
		defer func() { <-peer.bagsInitQueue }()
	}

	if sessionAttempt != nil {
		err = stNode.initializeSession(sessionAttempt, true)
		if err != nil {
			return err
		}
	}

	stNode.touch()

	Logger("[STORAGE] PEER CONNECTED", hex.EncodeToString(adnlID), peer.adnl.RemoteAddr(), "FOR", hex.EncodeToString(t.BagID), "PING", atomic.LoadInt64(&stNode.currentPing), "MS")

	for {
		ready := stNode.isSessionReady()
		usable := stNode.isDownloadUsable()
		if !ready && !usable && stNode.initProgressTimedOut(time.Now(), 45*time.Second) {
			Logger("[STORAGE_PEERS] PEER", hex.EncodeToString(stNode.nodeId), "HAS NOT SENT UPDATE INIT, SOMETHING WRONG, CLOSING CONNECTION", "BAG", hex.EncodeToString(t.BagID))
			stNode.Close()
			return fmt.Errorf("peer has not sent update init")
		} else if ready || usable {
			Logger("[STORAGE_PEERS] PEER", hex.EncodeToString(stNode.nodeId), "SESSION IS USABLE, BAG", hex.EncodeToString(t.BagID), "READY", ready, "KNOWN_PIECES", atomic.LoadUint32(&stNode.knownPieces))
			break
		}

		select {
		case <-ctx.Done():
			stNode.Close()
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}

	return nil
}

func (p *storagePeer) prepareTorrentInfo(ctx context.Context) error {
	p.prepareInfoMx.Lock() // to not request one peer in parallel
	defer p.prepareInfoMx.Unlock()

	p.torrent.mx.RLock()
	hasInfo := p.torrent.Info != nil
	p.torrent.mx.RUnlock()

	if !hasInfo {
		tm := time.Now()
		Logger("[STORAGE] REQUESTING TORRENT INFO FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(p.torrent.BagID))

		var res TorrentInfoContainer
		err := p.conn.withInitControlQueueSlot(ctx, true, func() error {
			infCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()

			return p.conn.rldp.DoQuery(infCtx, 1<<25, overlay.WrapQuery(p.overlay, &GetTorrentInfo{}), &res)
		})
		if err != nil {
			Logger("[STORAGE] ERR ", err.Error(), " REQUESTING TORRENT INFO FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(p.torrent.BagID))
			return err
		}
		Logger("[STORAGE] GOT TORRENT INFO TOOK", time.Since(tm).String(), "FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(p.torrent.BagID))

		cl, err := cell.FromBOC(res.Data)
		if err != nil {
			return fmt.Errorf("failed to parse torrent info boc: %w", err)
		}

		if !bytes.Equal(cl.Hash(), p.torrent.BagID) {
			return fmt.Errorf("incorrect torrent info")
		}

		var info TorrentInfo
		loader, err := cl.BeginParse()
		if err != nil {
			return fmt.Errorf("invalid torrent info cell")
		}
		err = tlb.LoadFromCell(&info, loader)
		if err != nil {
			return fmt.Errorf("invalid torrent info cell")
		}

		if info.PieceSize == 0 || info.HeaderSize == 0 {
			err = fmt.Errorf("incorrect torrent info sizes")
			return err
		}
		if info.HeaderSize > 20*1024*1024 {
			err = fmt.Errorf("too big header > 20 MB, looks dangerous")
			return err
		}
		if info.PieceSize < 4096 {
			err = fmt.Errorf("too small piece < 4 KB, cannot be handled")
			return err
		}
		if info.PieceSize > 8*1024*1024 {
			err = fmt.Errorf("too big piece > 8 MB, cannot be handled")
			return err
		}
		if info.PiecesNum() > 32000000 {
			err = fmt.Errorf("too many pieces > 32000000, cannot be handled")
			return err
		}

		p.torrent.mx.Lock()
		if p.torrent.Info == nil {
			p.torrent.Info = &info
		}
		p.torrent.mx.Unlock()

		p.torrent.InitMask()

		p.piecesMx.Lock()
		p.ensurePieceTrackingLocked(info.PiecesNum())
		p.piecesMx.Unlock()
	}

	p.torrent.mx.RLock()
	info := p.torrent.Info
	p.torrent.mx.RUnlock()
	if info != nil {
		complete, err := p.flushPendingPieceUpdates(info.PiecesNum())
		if err != nil {
			return err
		}
		if complete {
			p.markRemoteInitComplete()
		}
	}
	return nil
}

func (s *Server) startPeerSearcher() {
	ticker := time.NewTicker(peerSearcherInterval)
	defer ticker.Stop()

	updateSem := make(chan struct{}, s.dhtParallelism)
	updatePrioritySem := make(chan struct{}, s.dhtParallelism)

	tryAcquire := func(prioritized bool) (bool, bool) {
		if !prioritized {
			select {
			case updateSem <- struct{}{}:
				return false, true
			default:
				return false, false
			}
		}

		select {
		case updateSem <- struct{}{}:
			return false, true
		case updatePrioritySem <- struct{}{}:
			return true, true
		default:
			return false, false
		}
	}

	release := func(isPriority bool) {
		if !isPriority {
			<-updateSem
		} else {
			<-updatePrioritySem
		}
	}

	launchDHTJob := func(t *Torrent, findPeers, prioritized, seedRefresh bool) bool {
		isPr, ok := tryAcquire(prioritized)
		if !ok {
			return false
		}

		atomic.StoreInt64(&t.lastDHTStoreAt, time.Now().UnixNano())
		atomic.StoreInt32(&t.lastDHTStoreFailed, 0)

		go func() {
			defer func() {
				release(isPr)
				atomic.StoreInt64(&t.lastDHTStoreCompletedAt, time.Now().UnixNano())
			}()

			tm := time.Now()

			ctx, cancel := context.WithTimeout(t.globalCtx, time.Duration(90)*time.Second)
			var err error
			stored := 0
			if findPeers {
				err = s.checkAndUpdateBagDHT(ctx, t, s.serverMode)
			} else {
				stored, err = s.storeBagDHTSelf(ctx, t)
			}
			cancel()

			if err != nil {
				atomic.StoreInt32(&t.lastDHTStoreFailed, 1)
				atomic.AddUint32(&t.dhtStoreFailStreak, 1)
				Logger("[STORAGE] DHT QUERY BAG ERR", hex.EncodeToString(t.BagID), err.Error(), "TOOK", time.Since(tm).String())
				return
			}

			if !findPeers && stored > 0 {
				atomic.StoreInt64(&t.lastDHTStoreSuccessAt, time.Now().UnixNano())
			}

			if seedRefresh && stored < bagDHTSeedMinStoredNodes {
				atomic.StoreInt32(&t.lastDHTStoreFailed, 1)
				atomic.AddUint32(&t.dhtStoreFailStreak, 1)
				Logger("[STORAGE] BAG DHT RECORD LOW REPLICATION", hex.EncodeToString(t.BagID), "STORED", stored, "TOOK", time.Since(tm).String())
				return
			}

			atomic.StoreInt32(&t.lastDHTStoreFailed, 0)
			atomic.StoreUint32(&t.dhtStoreFailStreak, 0)
			Logger("[STORAGE] BAG DHT RECORD QUERY", hex.EncodeToString(t.BagID), "TOOK", time.Since(tm).String())
		}()
		return true
	}

	for {
		select {
		case <-s.closeCtx.Done():
			return
		case <-ticker.C:
		}

		if s.store == nil {
			continue
		}

		type seedCandidate struct {
			torrent *Torrent
			dueAt   time.Time
		}

		now := time.Now()
		seedCandidates := make([]seedCandidate, 0)
		seedCount := 0

		for _, t := range s.store.GetAll() {
			download, upload := t.IsActiveRaw()
			if !download && !upload {
				continue
			}

			completed := t.IsCompleted()
			if !upload && completed {
				continue
			}

			t.peersMx.RLock()
			numPeers := len(t.peers)
			usableDownloadPeers := 0
			for _, p := range t.peers {
				p.downloadSpeed.calculate(p.Downloaded)
				p.uploadSpeed.calculate(p.Uploaded)
				if p.peer != nil && p.peer.isDownloadUsable() {
					usableDownloadPeers++
				}
			}
			t.peersMx.RUnlock()

			if completed && upload {
				seedCount++

				lastAttemptAt := atomic.LoadInt64(&t.lastDHTStoreAt)
				lastCompletedAt := atomic.LoadInt64(&t.lastDHTStoreCompletedAt)
				if lastAttemptAt > 0 && lastAttemptAt > lastCompletedAt {
					continue
				}

				lastSuccessAt := atomic.LoadInt64(&t.lastDHTStoreSuccessAt)
				failed := atomic.LoadInt32(&t.lastDHTStoreFailed) != 0
				failStreak := atomic.LoadUint32(&t.dhtStoreFailStreak)
				at := bagDHTSeedNextAttemptAt(s.startedAt, t.BagID, lastCompletedAt, lastSuccessAt, failed, failStreak)
				if !now.Before(at) {
					seedCandidates = append(seedCandidates, seedCandidate{
						torrent: t,
						dueAt:   at,
					})
				}
				continue
			}

			if upload || !completed {
				activeDownload := !completed && download
				lastAttemptAt := atomic.LoadInt64(&t.lastDHTStoreAt)
				lastCompletedAt := atomic.LoadInt64(&t.lastDHTStoreCompletedAt)
				searchesWithZeroPeers := atomic.LoadUint32(&t.searchesWithZeroPeersNum)
				at := bagDHTNextAttemptAt(
					lastAttemptAt,
					lastCompletedAt,
					activeDownload,
					numPeers,
					usableDownloadPeers,
					s.serverMode,
					searchesWithZeroPeers,
					bagDHTAttemptJitter(activeDownload, numPeers, s.serverMode),
				)

				if time.Now().After(at) &&
					(lastAttemptAt == 0 || atomic.LoadInt32(&t.lastDHTStoreFailed) != 0 || lastAttemptAt < lastCompletedAt) {
					prioritized := activeDownload
					if t.CreatedLocally && t.CreatedAt.Add(15*time.Minute).After(time.Now()) {
						// ignore queue to made just created torrents available faster
						prioritized = true
					}

					if launchDHTJob(t, !completed, prioritized, false) {
						if activeDownload && usableDownloadPeers == 0 {
							atomic.AddUint32(&t.searchesWithZeroPeersNum, 1)
						} else if !activeDownload && numPeers == 0 {
							atomic.AddUint32(&t.searchesWithZeroPeersNum, 1)
						} else {
							atomic.StoreUint32(&t.searchesWithZeroPeersNum, 0)
						}

						Logger("[STORAGE] TIME TO MAKE BAG DHT RECORD QUERY", hex.EncodeToString(t.BagID), "PRIORITIZED:", prioritized)
					}
				}
			}
		}

		if len(seedCandidates) == 0 {
			continue
		}

		sort.Slice(seedCandidates, func(i, j int) bool {
			if !seedCandidates[i].dueAt.Equal(seedCandidates[j].dueAt) {
				return seedCandidates[i].dueAt.Before(seedCandidates[j].dueAt)
			}
			return bytes.Compare(seedCandidates[i].torrent.BagID, seedCandidates[j].torrent.BagID) < 0
		})

		launchBudget := bagDHTSeedLaunchBudget(seedCount, s.dhtParallelism)
		launched := 0
		for _, candidate := range seedCandidates {
			if launched >= launchBudget {
				break
			}

			if !launchDHTJob(candidate.torrent, false, false, true) {
				break
			}
			Logger("[STORAGE] TIME TO REFRESH BAG DHT RECORD", hex.EncodeToString(candidate.torrent.BagID), "DUE_AT", candidate.dueAt.Format(time.RFC3339))
			launched++
		}
	}
}

func (s *Server) GetID() []byte {
	return s.gate.GetID()
}

func (s *Server) GetADNLPrivateKey() ed25519.PrivateKey {
	return s.key
}

func (t *Torrent) prepareStoragePeer(over []byte, oNode *overlay.Node, conn *PeerConnection, sessionId *int64) (*storagePeer, *peerSessionAttempt) {
	t.mx.Lock()
	defer t.mx.Unlock()

	if n := conn.GetFor(t.BagID); n != nil {
		if sessionId == nil {
			if n.isDownloadUsable() || !n.initProgressTimedOut(time.Now(), 45*time.Second) {
				return n, nil
			}

			v := rand.Int63()
			sessionId = &v
		} else if atomic.LoadInt64(&n.session.sessionId) == *sessionId {
			return n, nil
		}

		if n.stopSession != nil {
			n.stopSession()
		}

		Logger("[STORAGE] REINITIALIZE REQUEST FOR", hex.EncodeToString(n.nodeId), "BAG", hex.EncodeToString(t.BagID), "OLD SESSION", atomic.LoadInt64(&n.session.sessionId), "NEW SESSION", *sessionId)

		generation := n.session.resetForReinit(*sessionId, time.Now())
		atomic.StoreUint64(&n.lastSentNewPiecesPos, 0)

		var piecesNum uint32
		if t.Info != nil {
			piecesNum = t.Info.PiecesNum()
		}
		n.piecesMx.Lock()
		n.resetPieceTrackingLocked(piecesNum)
		n.lastSentPieces = nil
		n.piecesMx.Unlock()

		var sessionCtx context.Context
		sessionCtx, n.stopSession = context.WithCancel(n.closerCtx)

		return n, newPeerSessionAttempt(sessionCtx, *sessionId, generation)
	}

	if sessionId == nil {
		v := rand.Int63()
		sessionId = &v
	}
	generation := uint64(1)

	stNode := &storagePeer{
		torrent:        t,
		nodeAddr:       conn.adnl.RemoteAddr(),
		nodeId:         conn.adnl.GetID(),
		conn:           conn,
		lastActivityAt: time.Now().UnixMilli(),
		overlay:        over,
		overlayNode:    oNode,
		session: peerSessionState{
			sessionId:     *sessionId,
			sessionGen:    generation,
			sessionInitAt: time.Now().UnixMilli(),
		},
	}
	stNode.closerCtx, stNode.stop = context.WithCancel(t.globalCtx)

	var sessionCtx context.Context
	sessionCtx, stNode.stopSession = context.WithCancel(stNode.closerCtx)

	var piecesNum uint32
	if t.Info != nil {
		piecesNum = t.Info.PiecesNum()
	}
	stNode.piecesMx.Lock()
	stNode.resetPieceTrackingLocked(piecesNum)
	stNode.lastSentPieces = nil
	stNode.piecesMx.Unlock()
	atomic.StoreUint64(&stNode.lastSentNewPiecesPos, 0)

	conn.UseFor(stNode)

	return stNode, newPeerSessionAttempt(sessionCtx, *sessionId, generation)
}

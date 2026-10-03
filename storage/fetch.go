package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type piecePack struct {
	data  []byte
	proof []byte
}

type PreFetcher struct {
	torrent            *Torrent
	ready              atomic.Int32
	processed          atomic.Int32
	prefetch           uint32
	pieceCursor        uint32
	roundRobin         bool
	pieces             map[uint32]*piecePack
	piecesList         []byte
	pendingPieces      []uint32
	pendingIndex       map[uint32]int
	pendingCursor      int
	pendingInitialized bool
	piecesChanged      chan struct{}

	report func(Event)

	mx    sync.RWMutex
	ctx   context.Context
	close func()
	wg    sync.WaitGroup
}

type Progress struct {
	Downloaded string
	Speed      string
}

func NewPreFetcher(ctx context.Context, torrent *Torrent, report func(Event), prefetch uint32, pieces []byte) *PreFetcher {
	return newPreFetcher(ctx, torrent, report, prefetch, pieces, false)
}

func NewUnorderedPreFetcher(ctx context.Context, torrent *Torrent, report func(Event), prefetch uint32, pieces []byte) *PreFetcher {
	return newPreFetcher(ctx, torrent, report, prefetch, pieces, true)
}

func newPreFetcher(ctx context.Context, torrent *Torrent, report func(Event), prefetch uint32, pieces []byte, roundRobin bool) *PreFetcher {
	ff := &PreFetcher{
		prefetch:   prefetch,
		torrent:    torrent,
		report:     report,
		roundRobin: roundRobin,
		piecesList: pieces,
		pieces:     map[uint32]*piecePack{},
	}
	ff.ctx, ff.close = context.WithCancel(ctx)

	for id, need := range pieces {
		// mark pieces as existing
		if need > 0 {
			ff.pieces[uint32(id)] = nil
		}
	}
	if roundRobin {
		ff.initPendingPiecesLocked()
	}

	ff.wg.Add(1)
	go func() {
		defer ff.wg.Done()
		ff.balancer()
	}()

	return ff
}

func (f *PreFetcher) Stop() {
	f.close()
	f.torrent.wake.fire()
	f.wg.Wait()
}

func (f *PreFetcher) WaitGet(ctx context.Context, piece uint32) ([]byte, []byte, error) {
	for {
		f.mx.Lock()
		p, ok := f.pieces[piece]
		if !ok {
			f.mx.Unlock()
			panic("unexpected piece requested")
		}
		if p != nil {
			f.mx.Unlock()
			return p.data, p.proof, nil
		}
		if f.piecesChanged == nil {
			f.piecesChanged = make(chan struct{})
		}
		changed := f.piecesChanged
		f.mx.Unlock()
		var stopped <-chan struct{}
		if f.ctx != nil {
			stopped = f.ctx.Done()
		}

		// wait for piece to be ready
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-stopped:
			return nil, nil, f.ctx.Err()
		case <-changed:
		}
	}
}

func (f *PreFetcher) Free(piece uint32) {
	f.mx.Lock()
	defer f.mx.Unlock()

	if _, ok := f.pieces[piece]; !ok {
		panic("unexpected piece requested")
	}
	delete(f.pieces, piece)
	f.processed.Add(1)
	f.torrent.wake.fire()
}

func (f *PreFetcher) markPieceForRetry(piece uint32) {
	if int(piece) >= len(f.piecesList) {
		return
	}
	if f.piecesList[piece] == 2 {
		f.piecesList[piece] = 1
		if f.roundRobin {
			f.initPendingPiecesLocked()
			if _, ok := f.pendingIndex[piece]; !ok {
				f.pendingIndex[piece] = len(f.pendingPieces)
				f.pendingPieces = append(f.pendingPieces, piece)
			}
		}
	}
	if !f.roundRobin && piece < f.pieceCursor {
		f.pieceCursor = piece
	}
}

func effectiveMaxInflightPieces(conn *PeerConnection) int32 {
	max := conn.MaxInflightPieces.Load()
	if max < 1 {
		max = 1
	}
	if conn.dataQueue == nil {
		return max
	}

	dataCap := int32(cap(conn.dataQueue))
	if dataCap > 0 && max > dataCap {
		return dataCap
	}
	return max
}

func dataQueueInflightCap(conn *PeerConnection) int32 {
	if conn.dataQueue == nil {
		return 0
	}
	return int32(cap(conn.dataQueue))
}

func (f *PreFetcher) nextPieceForPeer(peer *storagePeer, available int32) (uint32, bool) {
	if available <= 0 || len(f.piecesList) == 0 {
		return 0, false
	}
	if f.roundRobin {
		return f.nextRoundRobinPieceForPeer(peer)
	}

	start := int(f.pieceCursor)
	for start < len(f.piecesList) && f.piecesList[start] != 1 {
		start++
	}
	if start >= len(f.piecesList) {
		f.pieceCursor = uint32(len(f.piecesList))
		return 0, false
	}
	f.pieceCursor = uint32(start)

	for idx := start; idx < len(f.piecesList); idx++ {
		if f.piecesList[idx] != 1 {
			continue
		}

		if peer.hasPieceLocked(uint32(idx)) {
			return uint32(idx), true
		}
		available--
		if available <= 0 {
			break
		}
	}
	return 0, false
}

func (f *PreFetcher) nextRoundRobinPieceForPeer(peer *storagePeer) (uint32, bool) {
	f.initPendingPiecesLocked()
	if len(f.pendingPieces) == 0 {
		return 0, false
	}
	start := f.pendingCursor % len(f.pendingPieces)
	for off := 0; off < len(f.pendingPieces); off++ {
		idx := (start + off) % len(f.pendingPieces)
		piece := f.pendingPieces[idx]
		if f.piecesList[piece] != 1 {
			continue
		}
		if peer.hasPieceLocked(piece) {
			f.pendingCursor = (idx + 1) % len(f.pendingPieces)
			f.pieceCursor = piece + 1
			return piece, true
		}
	}
	return 0, false
}

func (f *PreFetcher) initPendingPiecesLocked() {
	if f.pendingInitialized {
		return
	}
	f.pendingInitialized = true
	f.pendingIndex = make(map[uint32]int)
	for id, need := range f.piecesList {
		if need == 1 {
			f.pendingIndex[uint32(id)] = len(f.pendingPieces)
			f.pendingPieces = append(f.pendingPieces, uint32(id))
			if uint32(id) < f.pieceCursor {
				f.pendingCursor++
			}
		}
	}
}

func (f *PreFetcher) reservePieceLocked(piece uint32) {
	f.piecesList[piece] = 2
	if !f.roundRobin {
		return
	}
	idx, ok := f.pendingIndex[piece]
	if !ok {
		return
	}
	last := len(f.pendingPieces) - 1
	lastPiece := f.pendingPieces[last]
	f.pendingPieces[idx] = lastPiece
	f.pendingIndex[lastPiece] = idx
	f.pendingPieces = f.pendingPieces[:last]
	delete(f.pendingIndex, piece)
	if idx < f.pendingCursor {
		f.pendingCursor = idx
	}
}

type nodeRTTInfo struct {
	LastRTT atomic.Int64

	mu         sync.Mutex
	srttMs     float64
	rttvarMs   float64
	minRttMs   float64
	lastSample time.Time
	failStreak int
}

func (n *nodeRTTInfo) onSuccessSample(rttMs int64, now time.Time) {
	const a = 1.0 / 8.0 //  srtt
	const b = 1.0 / 4.0 //  rttvar
	rm := float64(rttMs)

	n.mu.Lock()
	defer n.mu.Unlock()

	// minRTT
	if n.minRttMs == 0 || rm < n.minRttMs {
		n.minRttMs = rm
	}

	if n.srttMs == 0 {
		n.srttMs = rm
		n.rttvarMs = rm / 2
	} else {
		diff := math.Abs(n.srttMs - rm)
		n.rttvarMs = (1-b)*n.rttvarMs + b*diff
		n.srttMs = (1-a)*n.srttMs + a*rm
	}
	n.lastSample = now
	n.failStreak = 0
}

func (n *nodeRTTInfo) onFailure() {
	n.mu.Lock()
	n.failStreak++
	n.mu.Unlock()
}

func (n *nodeRTTInfo) snapshot() (srtt, rttvar, minrtt float64, last time.Time, fails int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.srttMs, n.rttvarMs, n.minRttMs, n.lastSample, n.failStreak
}

func (n *nodeRTTInfo) calcTimeout() time.Duration {
	srtt, rttvar, _, _, _ := n.snapshot()
	if srtt == 0 {
		return 15 * time.Second
	}

	if rttvar < srtt*0.12 {
		rttvar = srtt * 0.12
	}

	rtoMs := 3*srtt + 8*rttvar
	if rtoMs > 17000 {
		rtoMs = 17000
	}
	if rtoMs < 2500 {
		rtoMs = 2500
	}
	return time.Duration(rtoMs) * time.Millisecond
}

func (f *PreFetcher) balancer() {
	rttInfoMap := map[string]*nodeRTTInfo{}

	defer f.torrent.wake.fire()

	scoreOf := func(ok bool, rtt, fails int64, inflight, max int32) float64 {
		if rtt <= 0 {
			rtt = 3000
		}
		if max <= 0 {
			max = 1
		}
		load := float64(inflight+1) / float64(max)
		base := float64(rtt) * (1.0 + 0.9*load)

		if fails > 0 {
			exp := math.Pow(1.8, float64(fails)) // 1, 1.8, 3.24, 5.83, ...
			if exp > 16 {
				exp = 16
			}
			base = base*exp + float64(400*fails)
		}
		if !ok {
			base *= 1.8
			base += 800
		}

		if base < 1 {
			base = 1
		}
		return base
	}

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		peers := f.torrent.GetPeers()

		peersList := make([]*storagePeer, 0, len(peers))
		for _, p := range peers {
			if !p.peer.isDownloadUsable() {
				continue
			}
			peersList = append(peersList, p.peer)
		}

		// shuffle to prevent download same failed piece from same peer when conditions are similar
		rand.Shuffle(len(peersList), func(i, j int) {
			peersList[i], peersList[j] = peersList[j], peersList[i]
		})

		var piece uint32
		var bestNodeFails int
		var bestNode *storagePeer
		var bestNodeRttInfo *nodeRTTInfo
		var bestInflight int32
		for _, peer := range peersList {
			rttInfo := rttInfoMap[string(peer.nodeId)]
			if rttInfo == nil {
				rttInfo = &nodeRTTInfo{}
				rttInfoMap[string(peer.nodeId)] = rttInfo
			}

			_, _, _, _, fails := rttInfo.snapshot()
			available := (f.processed.Load() + int32(f.prefetch)) - f.ready.Load()
			if available <= 0 {
				break
			}

			curInflight := peer.conn.InflightPieces.Load()
			maxInflight := effectiveMaxInflightPieces(peer.conn)
			if curInflight >= maxInflight {
				continue
			}

			if bestNode != nil {
				thisScore := scoreOf(peer.conn.IsLastSuccess.Load(), rttInfo.LastRTT.Load(), int64(fails),
					curInflight, maxInflight)

				bestScore := scoreOf(bestNode.conn.IsLastSuccess.Load(), bestNodeRttInfo.LastRTT.Load(), int64(bestNodeFails),
					bestNode.conn.InflightPieces.Load(), effectiveMaxInflightPieces(bestNode.conn))

				// balance load
				if thisScore >= bestScore {
					continue
				}
			}

			peer.piecesMx.RLock()
			f.mx.Lock()
			nextPiece, ok := f.nextPieceForPeer(peer, available)
			f.mx.Unlock()
			peer.piecesMx.RUnlock()
			if ok {
				piece = nextPiece
				bestNode = peer
				bestInflight = curInflight
				bestNodeRttInfo = rttInfo
				bestNodeFails = fails
			}
		}

		if bestNode == nil {
			if !f.torrent.wake.wait(f.ctx) {
				return
			}
			continue
		}

		if !bestNode.conn.InflightPieces.CompareAndSwap(bestInflight, bestInflight+1) {
			// conflict, retry
			bestInflight = bestNode.conn.InflightPieces.Load()
			if bestInflight >= effectiveMaxInflightPieces(bestNode.conn) {
				// full
				continue
			}

			if !bestNode.conn.InflightPieces.CompareAndSwap(bestInflight, bestInflight+1) {
				// still no
				continue
			}
		}

		if err := bestNode.conn.srv.acquireDownloadSlot(f.ctx); err != nil {
			bestNode.conn.InflightPieces.Add(-1)
			bestNode.conn.wakeDownloaders()
			return
		}
		f.ready.Add(1) // add now to reserve slot

		for {
			// limit download speed, if configured
			err := f.torrent.connector.ThrottleDownload(f.ctx, uint64(f.torrent.Info.PieceSize))
			if err != nil {
				select {
				case <-f.ctx.Done():
					f.ready.Add(-1)
					bestNode.conn.InflightPieces.Add(-1)
					bestNode.conn.srv.releaseDownloadSlot()
					bestNode.conn.wakeDownloaders()
					return
				default:
					time.Sleep(2 * time.Millisecond)
					continue
				}
			}
			break
		}

		f.mx.Lock()
		f.reservePieceLocked(piece)
		f.mx.Unlock()

		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			defer bestNode.conn.wakeDownloaders()
			defer bestNode.conn.InflightPieces.Add(-1)
			defer bestNode.conn.srv.releaseDownloadSlot()

			tou := bestNodeRttInfo.calcTimeout()

			startedAt := time.Now()
			ctx, cancel := context.WithTimeout(f.ctx, tou)
			pc, rtt, err := bestNode.downloadPiece(ctx, piece)
			cancel()
			curMax := effectiveMaxInflightPieces(bestNode.conn)
			cur := bestNode.conn.InflightPieces.Load()
			now := time.Now()

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					bestNodeRttInfo.onFailure()
					bestNode.conn.IsLastSuccess.Store(false)
					if decision, changed := bestNode.conn.observeDownloadWindow(downloadWindowSample{
						startedAt: startedAt,
						duration:  time.Duration(rtt) * time.Millisecond,
						success:   false,
						timeout:   errors.Is(err, context.DeadlineExceeded),
						saturated: cur >= curMax,
					}); changed {
						logDownloadWindowChange(f.torrent, bestNode, decision)
					}
					Logger("[STORAGE] BAG", hex.EncodeToString(f.torrent.BagID), "PIECE", piece, "DOWNLOADED FAILED FROM", bestNode.conn.adnl.RemoteAddr(), hex.EncodeToString(bestNode.nodeId), "TOOK", rtt, "MS,", "INFLIGHT", bestNode.conn.InflightPieces.Load(), "MAX", bestNode.conn.MaxInflightPieces.Load(), "REASON", err.Error())
				}

				f.ready.Add(-1) // free slot to redownload

				f.mx.Lock()
				f.markPieceForRetry(piece)
				f.mx.Unlock()

				return
			}

			bestNode.conn.IsLastSuccess.Store(true)
			bestNodeRttInfo.LastRTT.Store(rtt)
			bestNodeRttInfo.onSuccessSample(rtt, now)
			if decision, changed := bestNode.conn.observeDownloadWindow(downloadWindowSample{
				startedAt: startedAt,
				duration:  time.Duration(rtt) * time.Millisecond,
				bytes:     uint64(len(pc.Data)),
				success:   true,
				saturated: cur >= curMax,
			}); changed {
				logDownloadWindowChange(f.torrent, bestNode, decision)
			}

			Logger("[STORAGE] BAG", hex.EncodeToString(f.torrent.BagID), "PIECE", piece, "DOWNLOADED FROM", bestNode.conn.adnl.RemoteAddr(), hex.EncodeToString(bestNode.nodeId), "TOOK", rtt, "MS,", "INFLIGHT", bestNode.conn.InflightPieces.Load(), "MAX", bestNode.conn.MaxInflightPieces.Load())

			f.mx.Lock()
			f.pieces[piece] = &piecePack{
				data:  pc.Data,
				proof: pc.Proof,
			}
			if f.piecesChanged != nil {
				close(f.piecesChanged)
				f.piecesChanged = nil
			}
			f.mx.Unlock()

			if f.report != nil {
				f.report(Event{Name: EventPieceDownloaded, Value: piece})
			}
		}()
	}
}

func logDownloadWindowChange(torrent *Torrent, peer *storagePeer, decision downloadWindowDecision) {
	Logger(
		"[STORAGE] DOWNLOAD WINDOW",
		hex.EncodeToString(peer.nodeId),
		peer.conn.adnl.RemoteAddr(),
		"BAG", hex.EncodeToString(torrent.BagID),
		"FROM", decision.oldWindow,
		"TO", decision.newWindow,
		"REASON", decision.reason,
		"GOODPUT", ToSpeed(uint64(decision.epoch.goodput())),
		"P50", decision.epoch.p50Ms, "MS",
		"P90", decision.epoch.p90Ms, "MS",
		"SUCCESS", decision.epoch.successes,
		"FAIL", decision.epoch.failures,
		"TIMEOUT", decision.epoch.timeouts,
		"LOSS", fmt.Sprintf("%.1f%%", decision.epoch.failureRate()*100),
	)
}

func ToSz(sz uint64) string {
	switch {
	case sz < 1024:
		return fmt.Sprintf("%d Bytes", sz)
	case sz < 1024*1024:
		return fmt.Sprintf("%.2f KB", float64(sz)/1024)
	case sz < 1024*1024*1024:
		return fmt.Sprintf("%.2f MB", float64(sz)/(1024*1024))
	default:
		return fmt.Sprintf("%.2f GB", float64(sz)/(1024*1024*1024))
	}
}

func ToSpeed(speed uint64) string {
	switch {
	case speed < 1024:
		return fmt.Sprintf("%d Bytes/s", speed)
	case speed < 1024*1024:
		return fmt.Sprintf("%.2f KB/s", float64(speed)/1024)
	case speed < 1024*1024*1024:
		return fmt.Sprintf("%.2f MB/s", float64(speed)/(1024*1024))
	default:
		return fmt.Sprintf("%.2f GB/s", float64(speed)/(1024*1024*1024))
	}
}

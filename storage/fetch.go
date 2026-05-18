package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type piecePack struct {
	data  []byte
	proof []byte
}

var (
	DownloadInitialPeerInflight  = int32(4)
	DownloadPeerInflightCap      = int32(32)
	DownloadSlowStartThreshold   = int32(16)
	DownloadSlowStartGrowthDiv   = int32(2)
	DownloadInflightChangeMinGap = 500 * time.Millisecond
)

type PreFetcher struct {
	torrent     *Torrent
	ready       atomic.Int32
	processed   atomic.Int32
	prefetch    uint32
	pieceCursor uint32
	roundRobin  bool
	pieces      map[uint32]*piecePack
	piecesList  []byte

	report func(Event)

	mx    sync.RWMutex
	ctx   context.Context
	close func()
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

	go ff.balancer()

	return ff
}

func (f *PreFetcher) Stop() {
	f.close()
	f.torrent.wake.fire()
}

func (f *PreFetcher) WaitGet(ctx context.Context, piece uint32) ([]byte, []byte, error) {
	f.mx.RLock()
	if _, ok := f.pieces[piece]; !ok {
		panic("unexpected piece requested")
	}
	f.mx.RUnlock()

	for {
		f.mx.RLock()
		if p := f.pieces[piece]; p != nil {
			f.mx.RUnlock()
			return p.data, p.proof, nil
		}
		f.mx.RUnlock()

		// wait for piece to be ready
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
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

func nextInflightAfterStable(actualMax, maxCap int32) int32 {
	if actualMax < 1 {
		actualMax = 1
	}

	next := actualMax + 1
	if DownloadSlowStartThreshold > 1 && actualMax < DownloadSlowStartThreshold {
		div := DownloadSlowStartGrowthDiv
		if div < 1 {
			div = 1
		}
		step := actualMax / div
		if step < 1 {
			step = 1
		}
		next = actualMax + step
		if next > DownloadSlowStartThreshold {
			next = DownloadSlowStartThreshold
		}
	}

	if maxCap > 0 && next > maxCap {
		return maxCap
	}
	return next
}

func inflightChangeGapMs(srtt float64) int64 {
	gap := int64(srtt * 1.2)
	minGap := DownloadInflightChangeMinGap.Milliseconds()
	if minGap < 0 {
		minGap = 0
	}
	if gap < minGap {
		gap = minGap
	}
	return gap
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
	start := int(f.pieceCursor % uint32(len(f.piecesList)))
	for off := 0; off < len(f.piecesList); off++ {
		idx := (start + off) % len(f.piecesList)
		if f.piecesList[idx] != 1 {
			continue
		}
		if peer.hasPieceLocked(uint32(idx)) {
			f.pieceCursor = uint32((idx + 1) % len(f.piecesList))
			return uint32(idx), true
		}
	}
	return 0, false
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

	scoreOf := func(ok bool, rtt, fails, upStreak, downStreak int64, inflight, max int32) float64 {
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

		if upStreak < 0 {
			upStreak = 0
		}
		if downStreak < 0 {
			downStreak = 0
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

		nowMs := time.Now().UnixMilli()

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
				thisScore := scoreOf(peer.conn.IsLastSuccess.Load(), rttInfo.LastRTT.Load(), int64(fails), peer.conn.UpStreak.Load(), peer.conn.DownStreak.Load(),
					curInflight, maxInflight)

				bestScore := scoreOf(bestNode.conn.IsLastSuccess.Load(), bestNodeRttInfo.LastRTT.Load(), int64(bestNodeFails),
					bestNode.conn.UpStreak.Load(), bestNode.conn.DownStreak.Load(),
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

		for {
			inf := bestNode.conn.srv.downloadInflight.Load()
			maxInf := bestNode.conn.srv.downloadMaxInflight.Load()
			if inf >= maxInf || !bestNode.conn.srv.downloadInflight.CompareAndSwap(inf, inf+1) {
				select {
				case <-f.ctx.Done():
					bestNode.conn.InflightPieces.Add(-1)
					return
				default:
					runtime.Gosched()
					continue
				}
			}

			break
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
					bestNode.conn.srv.downloadInflight.Add(-1)
					return
				default:
					time.Sleep(2 * time.Millisecond)
					continue
				}
			}
			break
		}

		f.mx.Lock()
		f.piecesList[piece] = 2
		f.mx.Unlock()

		go func() {
			defer f.torrent.wake.fire()
			defer bestNode.conn.InflightPieces.Add(-1)
			defer bestNode.conn.srv.downloadInflight.Add(-1)

			tou := bestNodeRttInfo.calcTimeout()

			ctx, cancel := context.WithTimeout(f.ctx, tou)
			pc, rtt, err := bestNode.downloadPiece(ctx, piece)
			cancel()
			lastChange := bestNode.conn.LastChange.Load()
			actualMax := bestNode.conn.MaxInflightPieces.Load()
			curMax := effectiveMaxInflightPieces(bestNode.conn)
			cur := bestNode.conn.InflightPieces.Load()

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					srtt, _, _, _, _ := bestNodeRttInfo.snapshot()
					minChangeMs := inflightChangeGapMs(srtt)

					uns := bestNode.conn.UnstableCount.Add(1)

					if uns >= 3 && actualMax > 1 && lastChange < time.Now().UnixMilli()-minChangeMs {
						newMax := actualMax - 1
						if nm := actualMax / 10; nm > 1 {
							// 10% reduce
							newMax = actualMax - nm
						}

						if bestNode.conn.MaxInflightPieces.CompareAndSwap(actualMax, newMax) {
							bestNode.conn.DownStreak.Add(1)
							bestNode.conn.UpStreak.Store(0)
							bestNode.conn.UnstableCount.Store(0)

							// fmt.Println("-- FAIL", bestNode.nodeAddr, rtt, curMax)

							bestNode.conn.LastChange.Store(time.Now().UnixMilli())
						}
					}

					// log.Println("-- X", bestNode.nodeAddr, rtt, cur, curMax, bestNode.conn.srv.downloadInflight.Load())

					bestNodeRttInfo.onFailure()

					bestNode.conn.StableCount.Store(0)
					bestNode.conn.IsLastSuccess.Store(false)
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
			now := time.Now()
			bestNodeRttInfo.onSuccessSample(rtt, now)

			srtt, _, minrtt, _, _ := bestNodeRttInfo.snapshot()

			minChangeMs := inflightChangeGapMs(srtt)

			/*var sp uint64
			for _, info := range f.torrent.GetPeers() {
				sp += uint64(info.downloadSpeed.speed)
			}*/

			if srtt > 0 {
				queueMs := float64(rtt) - minrtt
				diff := queueMs / (minrtt + 50)
				// diffPrc := fmt.Sprintf("%.2f", diff*100.0)
				// log.Println("-- Q", bestNode.nodeAddr, rtt, diffPrc, minrtt, cur, curMax, bestNode.conn.srv.downloadInflight.Load(), ToSpeed(sp))

				nowMs = now.UnixMilli()

				if diff < 0.25 || queueMs < 75 {
					// fmt.Println("-- STABLE", bestNode.nodeAddr, rtt, diffPrc, minrtt, cur, curMax, available, sp)

					if cur >= curMax {
						stable := bestNode.conn.StableCount.Add(1)
						bestNode.conn.UnstableCount.Store(0)

						need := curMax * 3
						if DownloadSlowStartThreshold > 1 && actualMax < DownloadSlowStartThreshold {
							need = curMax
						}
						if need < 2 {
							need = 2
						}

						if stable >= int64(need) && lastChange < nowMs-minChangeMs {
							maxCap := dataQueueInflightCap(bestNode.conn)
							newMax := nextInflightAfterStable(actualMax, maxCap)
							if newMax > actualMax {
								if bestNode.conn.MaxInflightPieces.CompareAndSwap(actualMax, newMax) {
									bestNode.conn.UpStreak.Add(1)
									bestNode.conn.DownStreak.Store(0)

									// fmt.Println("-- UP", bestNode.nodeAddr, rtt, diffPrc, minrtt, cur, curMax, available, sp)

									bestNode.conn.LastChange.Store(nowMs)
									bestNode.conn.StableCount.Store(0)
								}
							}
						}
					}
				} else if diff > 0.75 { // float64(rtt) >= decThresh || queueMs > 0.8*srtt
					uns := bestNode.conn.UnstableCount.Add(1)
					bestNode.conn.StableCount.Store(0)

					// fmt.Println("-- SLOW", bestNode.nodeAddr, rtt, diffPrc, minrtt, cur, curMax, sp)

					need := curMax / 2
					if need < 2 {
						need = 2
					}

					if uns >= int64(need) && lastChange < nowMs-minChangeMs {
						if actualMax > 1 && bestNode.conn.MaxInflightPieces.CompareAndSwap(actualMax, actualMax-1) {
							bestNode.conn.DownStreak.Add(1)
							bestNode.conn.UpStreak.Store(0)

							bestNode.conn.LastChange.Store(nowMs)
							bestNode.conn.UnstableCount.Store(0)

							// fmt.Println("-- DOWN", bestNode.nodeAddr, rtt, diffPrc, minrtt, cur, curMax, available, sp)
						}
					}
				} else {
					// fmt.Println("-- UNSTABLE", bestNode.nodeAddr, rtt, diffPrc, minrtt, cur, curMax, available, sp)

					bestNode.conn.StableCount.Store(0)
					bestNode.conn.UnstableCount.Store(0)
				}
			}

			Logger("[STORAGE] BAG", hex.EncodeToString(f.torrent.BagID), "PIECE", piece, "DOWNLOADED FROM", bestNode.conn.adnl.RemoteAddr(), hex.EncodeToString(bestNode.nodeId), "TOOK", rtt, "MS,", "INFLIGHT", bestNode.conn.InflightPieces.Load(), "MAX", bestNode.conn.MaxInflightPieces.Load())

			f.mx.Lock()
			f.pieces[piece] = &piecePack{
				data:  pc.Data,
				proof: pc.Proof,
			}
			f.mx.Unlock()

			if f.report != nil {
				f.report(Event{Name: EventPieceDownloaded, Value: piece})
			}
		}()
	}
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

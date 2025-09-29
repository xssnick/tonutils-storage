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
	torrent    *Torrent
	ready      atomic.Int32
	processed  atomic.Int32
	prefetch   uint32
	pieces     map[uint32]*piecePack
	piecesList []byte

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
	ff := &PreFetcher{
		prefetch:   prefetch,
		torrent:    torrent,
		report:     report,
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
}

type nodeRTTInfo struct {
	LastRTT       atomic.Int64
	StableCount   atomic.Int64
	IsLastSuccess atomic.Bool
	LastChange    atomic.Int64
	UpStreak      atomic.Int64
	DownStreak    atomic.Int64

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

func (n *nodeRTTInfo) onFailure(now time.Time) {
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

	rtoMs := 500 + 2*srtt + 8*rttvar
	if rtoMs > 15000 {
		rtoMs = 15000
	}
	return time.Duration(rtoMs) * time.Millisecond
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
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

		u := float64(min64(upStreak, 8))
		d := float64(min64(downStreak, 6))

		mult := math.Pow(0.90, u) * math.Pow(1.25, d)

		if mult < 0.6 {
			mult = 0.6
		}
		if mult > 3.0 {
			mult = 3.0
		}

		base *= mult

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
			if atomic.LoadInt32(&p.peer.sessionInitialized) == 0 {
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

			srtt, _, _, _, fails := rttInfo.snapshot()
			cooldownMs := int64(srtt)
			if cooldownMs < 100 {
				cooldownMs = 100
			}

			if fails > 1 {
				extra := cooldownMs * (1 << (fails - 1)) // 2^k
				if extra > 2000 {
					extra = 2000
				}
				cooldownMs += extra
			}

			if !rttInfo.IsLastSuccess.Load() && nowMs-rttInfo.LastChange.Load() < cooldownMs {
				continue
			}

			available := (f.processed.Load() + int32(f.prefetch)) - f.ready.Load()
			if available <= 0 {
				break
			}

			curInflight := peer.conn.inflightPieces.Load()
			maxInflight := peer.conn.maxInflightPieces.Load()
			if curInflight >= maxInflight {
				continue
			}

			if bestNode != nil {
				bestCurInflight := bestNode.conn.inflightPieces.Load()
				bestMaxInflight := bestNode.conn.maxInflightPieces.Load()

				thisScore := scoreOf(rttInfo.IsLastSuccess.Load(), rttInfo.LastRTT.Load(), int64(fails), rttInfo.UpStreak.Load(), rttInfo.DownStreak.Load(),
					curInflight, maxInflight)

				bestScore := scoreOf(bestNodeRttInfo.IsLastSuccess.Load(), bestNodeRttInfo.LastRTT.Load(), int64(bestNodeFails),
					bestNodeRttInfo.UpStreak.Load(), bestNodeRttInfo.DownStreak.Load(),
					bestCurInflight, bestMaxInflight)

				// balance load
				if thisScore >= bestScore {
					continue
				}
			}

			peer.piecesMx.RLock()
			f.mx.RLock()
			for pieceIndex, v := range f.piecesList {
				if v == 1 {
					available--

					if peer.hasPieces[uint32(pieceIndex)] {
						piece = uint32(pieceIndex)
						bestNode = peer
						bestInflight = curInflight
						bestNodeRttInfo = rttInfo
						bestNodeFails = fails
						break
					} else if available <= 0 {
						// wait for ready pieces processing before download new
						break
					}
				}
			}
			f.mx.RUnlock()
			peer.piecesMx.RUnlock()
		}

		if bestNode == nil {
			if !f.torrent.wake.wait(f.ctx) {
				return
			}
			continue
		}

		if !bestNode.conn.inflightPieces.CompareAndSwap(bestInflight, bestInflight+1) {
			// conflict, retry
			ni := bestNode.conn.inflightPieces.Load()
			if ni >= bestNode.conn.maxInflightPieces.Load() {
				// full
				continue
			}

			if !bestNode.conn.inflightPieces.CompareAndSwap(ni, ni+1) {
				// still no
				continue
			}
		}
		f.ready.Add(1) // add now to reserve slot

		for {
			// limit download speed, if configured
			err := f.torrent.connector.ThrottleDownload(f.ctx, uint64(f.torrent.Info.PieceSize))
			if err != nil {
				select {
				case <-f.ctx.Done():
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
			defer bestNode.conn.inflightPieces.Add(-1)

			ctx, cancel := context.WithTimeout(f.ctx, bestNodeRttInfo.calcTimeout())
			pc, rtt, err := bestNode.downloadPiece(ctx, piece)
			cancel()

			lastChange := bestNodeRttInfo.LastChange.Load()
			curMax := bestNode.conn.maxInflightPieces.Load()

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					srtt, _, _, _, _ := bestNodeRttInfo.snapshot()
					minChangeMs := int64(srtt * 1.2)
					if minChangeMs < 50 {
						minChangeMs = 50
					}

					if curMax > 1 && lastChange < time.Now().UnixMilli()-minChangeMs {
						bestNodeRttInfo.DownStreak.Add(1)
						bestNodeRttInfo.UpStreak.Add(0)

						// fmt.Println("-- FAIL", bestNode.nodeAddr, rtt, curMax)

						if bestNode.conn.maxInflightPieces.CompareAndSwap(curMax, curMax-1) {
							bestNodeRttInfo.LastChange.Store(time.Now().UnixMilli())
						}
					}

					bestNodeRttInfo.onFailure(time.Now())

					bestNodeRttInfo.StableCount.Store(0)
					bestNodeRttInfo.IsLastSuccess.Store(false)
					Logger("[STORAGE] BAG", hex.EncodeToString(f.torrent.BagID), "PIECE", piece, "DOWNLOADED FAILED FROM", bestNode.conn.adnl.RemoteAddr(), hex.EncodeToString(bestNode.nodeId), "TOOK", rtt, "MS,", "INFLIGHT", bestNode.conn.inflightPieces.Load(), "MAX", bestNode.conn.maxInflightPieces.Load(), "REASON", err.Error())
				}

				f.ready.Add(-1) // free slot to redownload

				f.mx.Lock()
				if f.piecesList[piece] == 2 {
					f.piecesList[piece] = 1
				}
				f.mx.Unlock()

				return
			}

			bestNodeRttInfo.IsLastSuccess.Store(true)
			bestNodeRttInfo.LastRTT.Store(rtt)
			bestNodeRttInfo.onSuccessSample(rtt, time.Now())

			srtt, rttvar, minrtt, _, _ := bestNodeRttInfo.snapshot()

			if srtt > 0 {
				queueMs := float64(rtt) - minrtt
				if queueMs < 0 {
					queueMs = 0
				}

				incThresh := srtt + 1.15*rttvar
				decThresh := srtt + 3*rttvar

				nowMs = time.Now().UnixMilli()

				if float64(rtt) <= incThresh && queueMs <= 0.25*srtt {
					stable := bestNodeRttInfo.StableCount.Add(1)

					minChangeMs := int64(srtt * 1.1)
					if minChangeMs < 50 {
						minChangeMs = 50
					}

					need := curMax
					if need < 1 {
						need = 1
					}

					if stable >= int64(need) && lastChange < nowMs-minChangeMs {
						if bestNode.conn.maxInflightPieces.CompareAndSwap(curMax, curMax+1) {
							bestNodeRttInfo.UpStreak.Add(1)
							bestNodeRttInfo.DownStreak.Add(0)

							// fmt.Println("-- UP", bestNode.nodeAddr, rtt, incThresh, curMax)

							bestNodeRttInfo.LastChange.Store(nowMs)
							bestNodeRttInfo.StableCount.Store(0)
						}
					}
				} else if float64(rtt) >= decThresh || queueMs > 0.8*srtt {
					bestNodeRttInfo.DownStreak.Add(1)
					bestNodeRttInfo.UpStreak.Add(0)

					// fmt.Println("-- DOWN", bestNode.nodeAddr, rtt, rttvar, srtt, queueMs, incThresh, decThresh, curMax)

					if curMax > 1 && bestNode.conn.maxInflightPieces.CompareAndSwap(curMax, curMax-1) {
						bestNodeRttInfo.LastChange.Store(nowMs)
					}
					bestNodeRttInfo.StableCount.Store(0)
				} else {
					bestNodeRttInfo.StableCount.Store(0)
				}
			}

			Logger("[STORAGE] BAG", hex.EncodeToString(f.torrent.BagID), "PIECE", piece, "DOWNLOADED FROM", bestNode.conn.adnl.RemoteAddr(), hex.EncodeToString(bestNode.nodeId), "TOOK", rtt, "MS,", "INFLIGHT", bestNode.conn.inflightPieces.Load(), "MAX", bestNode.conn.maxInflightPieces.Load())

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

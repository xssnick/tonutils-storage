package storage

import (
	"context"
	"fmt"
	"github.com/pterm/pterm"
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
	offset     int
	pieces     map[uint32]*piecePack
	tasks      chan uint32
	piecesList []uint32
	speed      uint64

	limitDownloadedUpdatedAt time.Time
	limitDownloaded          uint64
	bytesLimitPerSec         uint64
	limitMx                  sync.Mutex

	downloaded uint64
	report     func(Event)

	mx    sync.RWMutex
	ctx   context.Context
	close func()
}

type Progress struct {
	Downloaded string
	Speed      string
}

func NewPreFetcher(ctx context.Context, torrent *Torrent, report func(Event), downloaded uint64, threads, prefetch int, speedLimit uint64, pieces []uint32) *PreFetcher {
	if prefetch > len(pieces) {
		prefetch = len(pieces)
	}

	ff := &PreFetcher{
		torrent:          torrent,
		report:           report,
		piecesList:       pieces,
		downloaded:       downloaded,
		offset:           prefetch - 1,
		bytesLimitPerSec: speedLimit,
		pieces:           map[uint32]*piecePack{},
		tasks:            make(chan uint32, prefetch),
	}
	ff.ctx, ff.close = context.WithCancel(ctx)

	for i := 0; i < threads; i++ {
		go ff.worker()
	}

	go ff.speedometer()

	for _, piece := range pieces {
		// mark pieces as existing
		ff.pieces[piece] = nil
	}

	// pre-download pieces
	for i := 0; i < prefetch; i++ {
		ff.tasks <- ff.piecesList[i]
	}

	return ff
}

func (f *PreFetcher) Stop() {
	f.close()
}

func (f *PreFetcher) Get(ctx context.Context, piece uint32) ([]byte, []byte, error) {
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

	if f.offset+1 < len(f.piecesList) {
		f.offset++
		f.tasks <- f.piecesList[f.offset]
	}
}

func (f *PreFetcher) worker() {
	for {
		var task uint32
		select {
		case <-f.ctx.Done():
			return
		case task = <-f.tasks:
			/*if f.bytesLimitPerSec > 0 {
				f.limitMx.Lock()
				if f.limitDownloaded > f.bytesLimitPerSec {
					wait := time.Duration(float64(f.limitDownloaded) / float64(f.bytesLimitPerSec) * float64(time.Second))
					select {
					case <-f.ctx.Done():
						f.limitMx.Unlock()
						return
					case <-time.After(wait):
						f.limitDownloaded = 0
					}
				}
				f.limitDownloadedUpdatedAt = time.Now()
				f.limitDownloaded += uint64(f.torrent.Info.PieceSize)
				f.limitMx.Unlock()
			}*/
		}

		for {
			data, proof, peer, err := f.torrent.downloader.DownloadPieceDetailed(f.ctx, task)
			if err == nil {
				f.mx.Lock()
				f.pieces[task] = &piecePack{
					data:  data,
					proof: proof,
				}
				f.mx.Unlock()

				f.torrent.UpdateDownloadedPeer(peer, uint64(len(data)))

				atomic.AddUint64(&f.downloaded, 1)
				f.report(Event{Name: EventPieceDownloaded, Value: task})

				break
			}

			// when error we retry
			select {
			case <-f.ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
				pterm.Warning.Println("Piece", task, "download error (", err.Error(), "), will retry in 300ms")
			}
		}
	}
}

func (f *PreFetcher) speedometer() {
	first := f.downloaded
	next := first
	calcFrom := time.Now()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}

		downloaded := (atomic.LoadUint64(&f.downloaded) - first) * uint64(f.torrent.Info.PieceSize)

		period := uint64(time.Since(calcFrom) / (1000 * time.Millisecond))
		if period == 0 {
			period = 1
		}

		if time.Since(calcFrom) > 20*time.Second {
			first = next
			calcFrom = time.Now() //.Add(-5 * time.Second)
		} else if time.Since(calcFrom) >= 10*time.Second {
			next = atomic.LoadUint64(&f.downloaded)
		}

		f.speed = downloaded / period

		f.report(Event{Name: EventProgress, Value: Progress{
			Downloaded: ToSz(atomic.LoadUint64(&f.downloaded) * uint64(f.torrent.Info.PieceSize)),
			Speed:      ToSpeed(f.speed),
		}})
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

package downloader

import (
	"context"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/adnl/storage"
	"sync"
	"time"
)

type PreFetcher struct {
	dn        storage.TorrentDownloader
	prefetch  uint32
	offset    uint32
	pieces    map[uint32][]byte
	tasks     chan uint32
	lastPiece uint32
	pieceSize uint32
	speed     uint64

	progressMx sync.RWMutex
	pr         *pterm.ProgressbarPrinter

	mx    sync.RWMutex
	ctx   context.Context
	close func()
}

func NewPreFetcher(ctx context.Context, dn storage.TorrentDownloader, pr *pterm.ProgressbarPrinter, threads, prefetch, firstPiece, lastPiece uint32) *PreFetcher {
	ff := &PreFetcher{
		dn:        dn,
		pr:        pr,
		prefetch:  prefetch,
		lastPiece: lastPiece,
		offset:    firstPiece,
		pieces:    map[uint32][]byte{},
		tasks:     make(chan uint32, prefetch),
	}
	ff.ctx, ff.close = context.WithCancel(ctx)

	for i := uint32(0); i < threads; i++ {
		go ff.worker()
	}

	go ff.speedometer()

	// pre-download pieces
	for i := ff.offset; i <= ff.offset+prefetch; i++ {
		ff.tasks <- i
	}

	return ff
}

func (f *PreFetcher) Close() {
	f.close()
}

func (f *PreFetcher) Get(ctx context.Context, piece uint32) ([]byte, error) {
	if piece > f.offset+f.prefetch {
		panic("too far piece requested")
	}

	for {
		f.mx.RLock()
		p, ok := f.pieces[piece]
		if ok {
			f.mx.RUnlock()
			return p, nil
		}
		f.mx.RUnlock()

		// wait for piece to be ready
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (f *PreFetcher) Free(piece uint32) {
	f.mx.Lock()
	defer f.mx.Unlock()

	if f.offset != piece {
		panic("piece != offset (" + fmt.Sprint(f.offset) + "/" + fmt.Sprint(piece) + ")")
	}

	f.offset++
	delete(f.pieces, piece)

	if f.offset+f.prefetch <= f.lastPiece {
		f.tasks <- f.offset + f.prefetch
	}
}

func (f *PreFetcher) worker() {
	for {
		var task uint32
		select {
		case <-f.ctx.Done():
			return
		case task = <-f.tasks:
		}

		for {
			res, err := f.dn.DownloadPiece(f.ctx, task)
			if err == nil {
				f.mx.Lock()
				f.pieces[task] = res
				if f.pieceSize == 0 {
					f.pieceSize = uint32(len(res))
				}
				f.mx.Unlock()

				f.progressMx.Lock()
				f.pr.Increment()
				f.progressMx.Unlock()

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

func (f *PreFetcher) Downloaded() uint64 {
	return uint64(f.pr.Current) * uint64(f.pieceSize)
}

func (f *PreFetcher) Speed() uint64 {
	return f.speed
}

func (f *PreFetcher) speedometer() {
	first := 0
	next := first
	calcFrom := time.Now()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}

		downloaded := uint64(f.pr.Current-first) * uint64(f.pieceSize)

		period := uint64(time.Since(calcFrom) / (1000 * time.Millisecond))
		if period == 0 {
			period = 1
		}

		if time.Since(calcFrom) > 10*time.Second {
			first = next
			calcFrom = time.Now() //.Add(-5 * time.Second)
		} else if time.Since(calcFrom) >= 5*time.Second {
			next = f.pr.Current
		}

		f.speed = downloaded / period

		f.pr.UpdateTitle("Downloading pieces " +
			ToSpeed(f.speed) + " KB/s. Downloaded " + ToSz(uint64(f.pr.Current)*uint64(f.pieceSize)))
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

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
	downloader TorrentDownloader
	torrent    *Torrent
	offset     int
	pieces     map[uint32]*piecePack
	tasks      chan uint32
	piecesList []uint32
	speed      uint64

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

func NewPreFetcher(ctx context.Context, torrent *Torrent, downloader TorrentDownloader, report func(Event), downloaded uint64, threads, prefetch int, pieces []uint32) *PreFetcher {
	if prefetch > len(pieces) {
		prefetch = len(pieces)
	}

	ff := &PreFetcher{
		downloader: downloader,
		torrent:    torrent,
		report:     report,
		piecesList: pieces,
		downloaded: downloaded,
		offset:     prefetch - 1,
		pieces:     map[uint32]*piecePack{},
		tasks:      make(chan uint32, prefetch),
	}
	ff.ctx, ff.close = context.WithCancel(ctx)

	for i := 0; i < threads; i++ {
		go ff.worker()
	}

	// go ff.speedometer()

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
			err := f.torrent.connector.ThrottleDownload(f.ctx, uint64(f.torrent.Info.PieceSize))
			if err != nil {
				return
			}
		}

		for {
			data, proof, _, _, err := f.downloader.DownloadPieceDetailed(f.ctx, task)
			if err == nil {
				f.mx.Lock()
				f.pieces[task] = &piecePack{
					data:  data,
					proof: proof,
				}
				f.mx.Unlock()

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

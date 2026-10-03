package storage

import (
	"context"
	"runtime"
	"strings"
	"sync"
)

func (t *Torrent) verifyFilePieces(ctx context.Context, info *FileInfo, checked []bool) (bool, error) {
	var pending []uint32
	for id := info.FromPiece; id <= info.ToPiece; id++ {
		if !checked[id] {
			pending = append(pending, id)
		}
	}
	if len(pending) == 0 {
		return false, ctx.Err()
	}
	workers := min(len(pending), max(1, (runtime.NumCPU()/3)*2))
	workerCtx, cancel := context.WithCancel(ctx)
	type result struct {
		id  uint32
		err error
	}
	jobs := make(chan uint32, workers)
	results := make(chan result, workers)
	var wg sync.WaitGroup
	wg.Add(workers + 1)
	defer func() {
		cancel()
		wg.Wait()
	}()
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case id, ok := <-jobs:
					if !ok {
						return
					}
					_, err := t.getPieceInternal(id, true)
					select {
					case results <- result{id: id, err: err}:
					case <-workerCtx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		defer wg.Done()
		defer close(jobs)
		for _, id := range pending {
			select {
			case jobs <- id:
			case <-workerCtx.Done():
				return
			}
		}
	}()
	for range pending {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case result := <-results:
			if result.err != nil && !strings.Contains(result.err.Error(), "is not downloaded") {
				return true, nil
			}
			checked[result.id] = result.err == nil
		}
	}
	return false, nil
}

package storage

import (
	"context"
)

type wakeSig struct {
	ch chan struct{}
}

func newWakeSig() *wakeSig {
	return &wakeSig{ch: make(chan struct{}, 1)}
}

func (w *wakeSig) fire() {
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func (w *wakeSig) wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-w.ch:
		return true
	}
}

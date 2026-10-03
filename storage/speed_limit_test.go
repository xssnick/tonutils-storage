package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpeedLimitChargesOversizedPieceInFull(t *testing.T) {
	var limiter SpeedLimit
	limiter.SetLimit(1024)
	start := limiter.lastAt
	if err := limiter.throttleAt(1<<20, start); err != nil {
		t.Fatal(err)
	}
	for _, elapsed := range []time.Duration{3 * time.Second, 1023 * time.Second} {
		if err := limiter.throttleAt(1<<20, start.Add(elapsed)); err == nil {
			t.Fatalf("oversized reservation admitted again after %v", elapsed)
		}
	}
	if err := limiter.throttleAt(1<<20, start.Add(1024*time.Second)); err != nil {
		t.Fatalf("full reservation should refill after size/rate: %v", err)
	}
}

func TestSpeedLimitConcurrentReservationsRespectBurst(t *testing.T) {
	var limiter SpeedLimit
	limiter.SetLimit(4096)
	start := limiter.lastAt
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if limiter.throttleAt(4096, start) == nil {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if got := accepted.Load(); got != 3 {
		t.Fatalf("got %d reservations, want 3", got)
	}
	if err := limiter.throttleAt(4096, start.Add(time.Second)); err != nil {
		t.Fatal("one second should refill one reservation:", err)
	}
}

func TestSpeedLimitCancellationAndDisabledLimit(t *testing.T) {
	var limiter SpeedLimit
	limiter.SetLimit(1024)
	limiter.SetLimit(0)
	if err := limiter.Throttle(context.Background(), 1<<20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limiter.Throttle(ctx, 1); err != context.Canceled {
		t.Fatalf("got %v, want canceled", err)
	}
}

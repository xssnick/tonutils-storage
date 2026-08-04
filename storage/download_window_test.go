package storage

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestDownloadWindowControllerProbe(t *testing.T) {
	tests := []struct {
		name       string
		base       downloadWindowEpoch
		probe      []downloadWindowEpoch
		wantWindow int32
		wantReason string
	}{
		{
			name:       "accepts goodput gain with bounded latency",
			base:       testDownloadWindowEpoch(10, 300, 8),
			probe:      []downloadWindowEpoch{testDownloadWindowEpoch(11, 360, 12)},
			wantWindow: 9,
			wantReason: "warmup",
		},
		{
			name: "rolls back after two epochs without gain",
			base: testDownloadWindowEpoch(10, 300, 8),
			probe: []downloadWindowEpoch{
				testDownloadWindowEpoch(10, 320, 12),
				testDownloadWindowEpoch(10, 330, 12),
			},
			wantWindow: 4,
			wantReason: "probe_no_gain",
		},
		{
			name: "rejects gain with excessive latency",
			base: testDownloadWindowEpoch(10, 300, 8),
			probe: []downloadWindowEpoch{
				testDownloadWindowEpoch(12, 500, 12),
				testDownloadWindowEpoch(12, 520, 12),
			},
			wantWindow: 4,
			wantReason: "probe_no_gain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var controller downloadWindowController
			window := int32(4)

			decision := controller.evaluate(window, 32, tt.base)
			if decision.newWindow != 6 {
				t.Fatalf("expected warm-up probe from 4 to 6, got %d", decision.newWindow)
			}
			window = decision.newWindow

			for _, epoch := range tt.probe {
				decision = controller.evaluate(window, 32, epoch)
				window = decision.newWindow
			}

			if window != tt.wantWindow {
				t.Fatalf("unexpected window after probe: got %d, want %d", window, tt.wantWindow)
			}
			if decision.reason != tt.wantReason {
				t.Fatalf("unexpected final reason: got %q, want %q", decision.reason, tt.wantReason)
			}
		})
	}
}

func TestDownloadWindowControllerStableLossCanGrow(t *testing.T) {
	var controller downloadWindowController
	window := int32(4)

	decision := controller.evaluate(window, 32, testDownloadWindowEpochWithLoss(10, 300, 10, 0.20))
	window = decision.newWindow
	if window != 6 {
		t.Fatalf("expected warm-up under stable loss, got window %d", window)
	}

	decision = controller.evaluate(window, 32, testDownloadWindowEpochWithLoss(12, 320, 14, 0.20))
	if decision.newWindow != 9 {
		t.Fatalf("stable random loss collapsed growth: got %d, want 9", decision.newWindow)
	}
}

func TestDownloadWindowControllerFailureHandling(t *testing.T) {
	t.Run("single failure does not reduce", func(t *testing.T) {
		var controller downloadWindowController
		epoch := testDownloadWindowEpoch(10, 300, 10)
		epoch.failures = 1

		decision := controller.evaluate(10, 32, epoch)
		if decision.newWindow < 10 {
			t.Fatalf("single failure reduced window to %d", decision.newWindow)
		}
	})

	t.Run("two complete timeouts reduce immediately", func(t *testing.T) {
		var controller downloadWindowController
		epoch := downloadWindowEpoch{
			duration: time.Second,
			failures: downloadWindowSevereFailures,
			timeouts: downloadWindowSevereFailures,
		}

		decision := controller.evaluate(10, 32, epoch)
		if decision.newWindow != 5 {
			t.Fatalf("complete timeouts should halve window: got %d, want 5", decision.newWindow)
		}
		if decision.reason != "timeouts" {
			t.Fatalf("unexpected reason: got %q, want timeouts", decision.reason)
		}
	})
}

func TestDownloadWindowControllerSuccessfulTraceDoesNotCollapse(t *testing.T) {
	// One-second aggregates from the supplied production trace. The previous
	// controller reduced the window from 6 to 3 although every piece succeeded.
	trace := []struct {
		pieces int
		p90Ms  int64
	}{
		{pieces: 18, p90Ms: 544},
		{pieces: 17, p90Ms: 795},
		{pieces: 13, p90Ms: 818},
		{pieces: 16, p90Ms: 727},
		{pieces: 18, p90Ms: 544},
		{pieces: 12, p90Ms: 724},
		{pieces: 11, p90Ms: 719},
		{pieces: 10, p90Ms: 437},
		{pieces: 9, p90Ms: 463},
	}

	var controller downloadWindowController
	window := int32(6)
	minWindow := window
	maxWindow := window
	for _, sample := range trace {
		decision := controller.evaluate(
			window,
			32,
			testDownloadWindowEpoch(sample.pieces, sample.p90Ms, max(sample.pieces, int(window))),
		)
		window = decision.newWindow
		minWindow = min(minWindow, window)
		maxWindow = max(maxWindow, window)
	}

	if minWindow != 6 {
		t.Fatalf("successful trace collapsed the window to %d, want floor 6", minWindow)
	}
	if maxWindow > 9 {
		t.Fatalf("successful trace probed too aggressively up to %d, want at most 9", maxWindow)
	}
}

func TestDownloadWindowControllerObservesEpoch(t *testing.T) {
	oldMin := DownloadWindowEpochMin
	oldMax := DownloadWindowEpochMax
	defer func() {
		DownloadWindowEpochMin = oldMin
		DownloadWindowEpochMax = oldMax
	}()

	DownloadWindowEpochMin = 100 * time.Millisecond
	DownloadWindowEpochMax = 100 * time.Millisecond

	var controller downloadWindowController
	start := time.Unix(100, 0)
	for i := range 4 {
		decision, ready := controller.observe(
			downloadWindowSample{
				startedAt: start,
				at:        start.Add(250*time.Millisecond + time.Duration(i)*40*time.Millisecond),
				duration:  250 * time.Millisecond,
				bytes:     1024,
				success:   true,
				saturated: true,
			},
			4,
			32,
		)
		if i < 3 && ready {
			t.Fatalf("epoch finished too early at sample %d", i)
		}
		if i == 3 {
			if !ready {
				t.Fatal("expected epoch decision after hard interval")
			}
			if decision.newWindow != 6 {
				t.Fatalf("expected warm-up probe to 6, got %d", decision.newWindow)
			}
			if decision.epoch.p50Ms != 250 || decision.epoch.p90Ms != 250 {
				t.Fatalf(
					"unexpected latency summary p50=%d p90=%d",
					decision.epoch.p50Ms,
					decision.epoch.p90Ms,
				)
			}
		}
	}
}

func TestDownloadWindowPercentileUsesNearestRank(t *testing.T) {
	samples := []int64{10, 20, 30, 1000}
	if got := percentile(samples, 90); got != 1000 {
		t.Fatalf("p90 should include tail sample: got %d, want 1000", got)
	}
}

func TestDownloadWindowControllerNetworkModels(t *testing.T) {
	tests := []struct {
		name              string
		network           testDownloadNetwork
		wantCapacityRatio float64
		wantMaxWindow     int32
	}{
		{
			name: "fast low-loss link",
			network: testDownloadNetwork{
				bandwidth: 64 << 20,
				baseRTT:   200 * time.Millisecond,
			},
			wantCapacityRatio: 0.90,
			wantMaxWindow:     19,
		},
		{
			name: "slow high-loss link",
			network: testDownloadNetwork{
				bandwidth: 2 << 20,
				baseRTT:   500 * time.Millisecond,
				lossRate:  0.20,
			},
			wantCapacityRatio: 0.95,
			wantMaxWindow:     6,
		},
		{
			name: "high-BDP link with stable loss",
			network: testDownloadNetwork{
				bandwidth: 20 << 20,
				baseRTT:   500 * time.Millisecond,
				lossRate:  0.20,
			},
			wantCapacityRatio: 0.85,
			wantMaxWindow:     13,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := simulateDownloadWindow(tt.network, 20)
			capacity := tt.network.bandwidth * (1 - tt.network.lossRate)
			if result.maxGoodput < capacity*tt.wantCapacityRatio {
				t.Fatalf(
					"goodput %.2f MB/s is below %.0f%% of capacity %.2f MB/s (windows %v)",
					result.maxGoodput/(1<<20),
					tt.wantCapacityRatio*100,
					capacity/(1<<20),
					result.windows,
				)
			}
			if result.maxWindow > tt.wantMaxWindow {
				t.Fatalf("window grew to %d, want at most %d (windows %v)", result.maxWindow, tt.wantMaxWindow, result.windows)
			}
			if result.minWindow < 4 {
				t.Fatalf("stable link collapsed below initial window: %v", result.windows)
			}
		})
	}
}

func TestDownloadWindowControllerAdaptsToBandwidthDrop(t *testing.T) {
	fast := testDownloadNetwork{bandwidth: 64 << 20, baseRTT: 200 * time.Millisecond}
	slowed := testDownloadNetwork{bandwidth: 16 << 20, baseRTT: 200 * time.Millisecond}

	var controller downloadWindowController
	window := int32(4)
	for range 10 {
		window = controller.evaluate(window, 32, fast.epoch(window)).newWindow
	}
	windowBeforeDrop := window
	for range 16 {
		window = controller.evaluate(window, 32, slowed.epoch(window)).newWindow
	}

	if windowBeforeDrop < 12 {
		t.Fatalf("controller did not warm up before drop: got %d", windowBeforeDrop)
	}
	if window > 6 {
		t.Fatalf("controller did not drain queue after bandwidth drop: %d -> %d", windowBeforeDrop, window)
	}
	if goodput := slowed.goodput(window); goodput < slowed.bandwidth*0.90 {
		t.Fatalf("reduced window leaves link underutilized: %.2f MB/s", goodput/(1<<20))
	}
}

func TestPeerConnectionDownloadWindowConcurrentObservations(t *testing.T) {
	oldMin := DownloadWindowEpochMin
	oldMax := DownloadWindowEpochMax
	defer func() {
		DownloadWindowEpochMin = oldMin
		DownloadWindowEpochMax = oldMax
	}()
	DownloadWindowEpochMin = time.Hour
	DownloadWindowEpochMax = time.Hour

	conn := &PeerConnection{
		dataQueue: make(chan struct{}, 32),
	}
	conn.MaxInflightPieces.Store(4)

	var wg sync.WaitGroup
	for range 128 {
		wg.Go(func() {
			conn.observeDownloadWindow(downloadWindowSample{
				startedAt: time.Now(),
				duration:  250 * time.Millisecond,
				bytes:     1 << 20,
				success:   true,
				saturated: true,
			})
		})
	}
	wg.Wait()

	if window := conn.MaxInflightPieces.Load(); window != 4 {
		t.Fatalf("window changed before an epoch elapsed: got %d, want 4", window)
	}
}

func BenchmarkDownloadWindowControllerObserve(b *testing.B) {
	var controller downloadWindowController
	window := int32(4)
	now := time.Unix(100, 0)

	b.ReportAllocs()
	for b.Loop() {
		now = now.Add(50 * time.Millisecond)
		decision, ready := controller.observe(downloadWindowSample{
			startedAt: now.Add(-250 * time.Millisecond),
			at:        now,
			duration:  250 * time.Millisecond,
			bytes:     1 << 20,
			success:   true,
			saturated: true,
		}, window, 32)
		if ready {
			window = decision.newWindow
		}
	}
}

type testDownloadNetwork struct {
	bandwidth float64
	baseRTT   time.Duration
	lossRate  float64
}

func (n testDownloadNetwork) goodput(window int32) float64 {
	const pieceSize = float64(1 << 20)
	windowLimited := float64(window) * pieceSize / n.baseRTT.Seconds()
	return min(n.bandwidth, windowLimited) * (1 - n.lossRate)
}

func (n testDownloadNetwork) epoch(window int32) downloadWindowEpoch {
	const pieceSize = float64(1 << 20)
	samples := max(8, int(window)*2)
	failures := int(math.Round(float64(samples) * n.lossRate))
	successes := max(1, samples-failures)
	queueLatency := time.Duration(float64(window) * pieceSize / n.bandwidth * float64(time.Second))
	latency := max(n.baseRTT, queueLatency)

	return downloadWindowEpoch{
		duration:    time.Second,
		successes:   successes,
		failures:    failures,
		saturated:   successes,
		bytes:       uint64(n.goodput(window)),
		p50Ms:       latency.Milliseconds(),
		p90Ms:       latency.Milliseconds(),
		lossRate:    n.lossRate,
		lossRateSet: true,
	}
}

type testDownloadWindowResult struct {
	maxGoodput float64
	minWindow  int32
	maxWindow  int32
	windows    []int32
}

func simulateDownloadWindow(network testDownloadNetwork, epochs int) testDownloadWindowResult {
	var controller downloadWindowController
	window := int32(4)
	result := testDownloadWindowResult{
		minWindow: window,
		maxWindow: window,
		windows:   make([]int32, 0, epochs+1),
	}
	result.windows = append(result.windows, window)
	for range epochs {
		result.maxGoodput = max(result.maxGoodput, network.goodput(window))
		window = controller.evaluate(window, 32, network.epoch(window)).newWindow
		result.minWindow = min(result.minWindow, window)
		result.maxWindow = max(result.maxWindow, window)
		result.windows = append(result.windows, window)
	}
	return result
}

func testDownloadWindowEpoch(goodput int, p90Ms int64, samples int) downloadWindowEpoch {
	return downloadWindowEpoch{
		duration:  time.Second,
		successes: samples,
		saturated: samples,
		bytes:     uint64(goodput),
		p50Ms:     p90Ms / 2,
		p90Ms:     p90Ms,
	}
}

func testDownloadWindowEpochWithLoss(
	goodput int,
	p90Ms int64,
	samples int,
	lossRate float64,
) downloadWindowEpoch {
	epoch := testDownloadWindowEpoch(goodput, p90Ms, samples)
	epoch.failures = int(math.Round(float64(samples) * lossRate))
	epoch.successes = samples - epoch.failures
	epoch.saturated = epoch.successes
	epoch.lossRate = lossRate
	epoch.lossRateSet = true
	return epoch
}

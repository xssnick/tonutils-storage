package storage

import (
	"slices"
	"time"
)

var (
	DownloadInitialPeerInflight = int32(4)
	DownloadPeerInflightCap     = int32(32)

	DownloadWindowEpochMin = 500 * time.Millisecond
	DownloadWindowEpochMax = 2 * time.Second

	// Deprecated: the download window now uses multiplicative warm-up followed
	// by bounded additive probes. These values are kept for source compatibility.
	DownloadSlowStartThreshold = int32(16)
	DownloadSlowStartGrowthDiv = int32(2)

	// Deprecated: use DownloadWindowEpochMin.
	DownloadInflightChangeMinGap = 500 * time.Millisecond
)

const (
	downloadWindowProbeMinGain       = 0.05
	downloadWindowProbeEpochs        = 2
	downloadWindowCooldownEpochs     = 2
	downloadWindowMaxLatencySamples  = 1024
	downloadWindowLatencyMultiplier  = 4
	downloadWindowLatencyRisePercent = 25
	downloadWindowLatencyRiseFloorMs = int64(75)
	downloadWindowSevereLatencyRatio = 2
	downloadWindowLossRiseFloor      = 0.03
	downloadWindowGoodputDrop        = 0.10
	downloadWindowSteadyGrowthDiv    = int32(8)
	downloadWindowSteadyGrowthCap    = int32(4)
	downloadWindowDecreaseDiv        = int32(5)
	// Two requests timing out without a single delivery is a strong signal and
	// should not wait for a complete epoch.
	downloadWindowSevereFailures = 2
)

type downloadWindowSample struct {
	startedAt time.Time
	at        time.Time
	duration  time.Duration
	bytes     uint64
	success   bool
	timeout   bool
	saturated bool
}

type downloadWindowEpoch struct {
	duration    time.Duration
	successes   int
	failures    int
	timeouts    int
	saturated   int
	bytes       uint64
	p50Ms       int64
	p90Ms       int64
	lossRate    float64
	lossRateSet bool
}

func (e downloadWindowEpoch) goodput() float64 {
	seconds := e.duration.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(e.bytes) / seconds
}

func (e downloadWindowEpoch) samples() int {
	return e.successes + e.failures
}

func (e downloadWindowEpoch) failureRate() float64 {
	if e.lossRateSet {
		return e.lossRate
	}
	total := e.samples()
	if total == 0 {
		return 0
	}
	return float64(e.failures) / float64(total)
}

type downloadWindowDecision struct {
	oldWindow int32
	newWindow int32
	reason    string
	epoch     downloadWindowEpoch
}

func (d downloadWindowDecision) changed() bool {
	return d.newWindow != d.oldWindow
}

type downloadWindowProbe struct {
	active     bool
	warmup     bool
	fromWindow int32
	goodput    float64
	p90Ms      int64
	lossRate   float64
	epochs     int
}

type downloadWindowController struct {
	epochStarted time.Time
	successes    int
	failures     int
	timeouts     int
	saturated    int
	bytes        uint64
	latenciesMs  []int64

	latencyEWMA float64
	reference   downloadWindowEpoch
	bestWindow  int32
	probe       downloadWindowProbe
	cooldown    int
	badEpochs   int
	steady      bool
}

func (c *downloadWindowController) observe(
	sample downloadWindowSample,
	window int32,
	windowCap int32,
) (downloadWindowDecision, bool) {
	if c.epochStarted.IsZero() {
		c.epochStarted = downloadSampleStartedAt(sample)
	}

	if sample.success {
		c.successes++
		c.bytes += sample.bytes
		if sample.saturated {
			c.saturated++
		}
		c.addLatencySample(sample.duration.Milliseconds())
	} else {
		c.failures++
		if sample.timeout {
			c.timeouts++
		}
	}

	elapsed := sample.at.Sub(c.epochStarted)
	total := c.successes + c.failures
	targetSamples := max(8, int(max(window, int32(1)))*2)
	minimumSamples := max(4, int(max(window, int32(1))))
	interval := c.controlInterval()
	hardInterval := max(2*interval, normalizedDownloadWindowEpochMax())

	epochReady := elapsed >= interval && total >= targetSamples
	if elapsed >= hardInterval && total >= minimumSamples {
		epochReady = true
	}
	if c.successes == 0 && c.timeouts >= downloadWindowSevereFailures && elapsed >= normalizedDownloadWindowEpochMin() {
		epochReady = true
	}
	if !epochReady {
		return downloadWindowDecision{}, false
	}

	epoch := c.finishEpoch(sample.at)
	return c.evaluate(window, windowCap, epoch), true
}

func downloadSampleStartedAt(sample downloadWindowSample) time.Time {
	startedAt := sample.startedAt
	if startedAt.IsZero() && sample.duration > 0 {
		startedAt = sample.at.Add(-sample.duration)
	}
	if startedAt.IsZero() || startedAt.After(sample.at) {
		return sample.at
	}
	return startedAt
}

func (c *downloadWindowController) addLatencySample(latencyMs int64) {
	if len(c.latenciesMs) < downloadWindowMaxLatencySamples {
		c.latenciesMs = append(c.latenciesMs, latencyMs)
		return
	}

	c.latenciesMs[(c.successes-1)%len(c.latenciesMs)] = latencyMs
}

func normalizedDownloadWindowEpochMin() time.Duration {
	if DownloadWindowEpochMin <= 0 {
		return 500 * time.Millisecond
	}
	return DownloadWindowEpochMin
}

func normalizedDownloadWindowEpochMax() time.Duration {
	minimum := normalizedDownloadWindowEpochMin()
	if DownloadWindowEpochMax < minimum {
		return minimum
	}
	return DownloadWindowEpochMax
}

func (c *downloadWindowController) controlInterval() time.Duration {
	minimum := normalizedDownloadWindowEpochMin()
	maximum := normalizedDownloadWindowEpochMax()
	interval := time.Duration(c.latencyEWMA*downloadWindowLatencyMultiplier) * time.Millisecond
	return min(max(interval, minimum), maximum)
}

func (c *downloadWindowController) finishEpoch(now time.Time) downloadWindowEpoch {
	latencies := slices.Clone(c.latenciesMs)
	slices.Sort(latencies)

	total := c.successes + c.failures
	lossRate := 0.0
	if total > 0 {
		lossRate = float64(c.failures) / float64(total)
	}
	epoch := downloadWindowEpoch{
		duration:    now.Sub(c.epochStarted),
		successes:   c.successes,
		failures:    c.failures,
		timeouts:    c.timeouts,
		saturated:   c.saturated,
		bytes:       c.bytes,
		p50Ms:       percentile(latencies, 50),
		p90Ms:       percentile(latencies, 90),
		lossRate:    lossRate,
		lossRateSet: true,
	}

	if epoch.p50Ms > 0 {
		if c.latencyEWMA == 0 {
			c.latencyEWMA = float64(epoch.p50Ms)
		} else {
			c.latencyEWMA = 0.75*c.latencyEWMA + 0.25*float64(epoch.p50Ms)
		}
	}

	c.epochStarted = now
	c.successes = 0
	c.failures = 0
	c.timeouts = 0
	c.saturated = 0
	c.bytes = 0
	c.latenciesMs = c.latenciesMs[:0]

	return epoch
}

func percentile(sorted []int64, percent int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if percent <= 0 {
		return sorted[0]
	}
	if percent >= 100 {
		return sorted[len(sorted)-1]
	}

	// Nearest-rank percentile includes the tail sample for small epochs. The
	// previous floor-based formula reported the third sample of four as p90.
	index := (len(sorted)*percent+99)/100 - 1
	return sorted[index]
}

func (c *downloadWindowController) evaluate(
	window int32,
	windowCap int32,
	epoch downloadWindowEpoch,
) downloadWindowDecision {
	if window < 1 {
		return downloadWindowDecision{
			oldWindow: window,
			newWindow: 1,
			reason:    "minimum",
			epoch:     epoch,
		}
	}

	decision := downloadWindowDecision{
		oldWindow: window,
		newWindow: window,
		epoch:     epoch,
	}
	if windowCap > 0 && window > windowCap {
		decision.newWindow = max(int32(1), windowCap)
		decision.reason = "cap"
		c.resetAdaptation()
		return decision
	}

	if epoch.successes == 0 && epoch.timeouts >= downloadWindowSevereFailures {
		decision.newWindow = max(int32(1), window/2)
		decision.reason = "timeouts"
		c.afterDecrease()
		return decision
	}

	minimumSamples := max(4, int(window))
	if epoch.successes < minimumSamples || epoch.saturated*2 < epoch.successes {
		return decision
	}

	if c.reference.successes > 0 {
		throughputWorse := epoch.goodput() < c.reference.goodput()*(1-downloadWindowGoodputDrop)
		latencyWorse := latencyRegressed(epoch.p90Ms, c.reference.p90Ms)
		strongCongestionSignal := severeLatencyRegressed(epoch.p90Ms, c.reference.p90Ms) ||
			epoch.failures > 0 && latencyWorse
		if throughputWorse && strongCongestionSignal {
			c.badEpochs++
			if c.badEpochs >= 2 {
				decision.newWindow = decreasedDownloadWindow(window)
				if c.probe.active && c.probe.fromWindow < decision.newWindow {
					decision.newWindow = c.probe.fromWindow
				}
				decision.reason = "congestion"
				c.afterDecrease()
			}
			return decision
		}
	}
	c.badEpochs = 0

	if c.probe.active {
		return c.evaluateProbe(decision, windowCap)
	}

	if c.cooldown > 0 {
		c.cooldown--
		return decision
	}

	if c.reference.successes == 0 {
		c.setReference(epoch, window)
	} else {
		c.updateReference(epoch, window)
	}

	if windowCap > 0 && window >= windowCap {
		return decision
	}
	return c.startProbe(decision, windowCap)
}

func (c *downloadWindowController) startProbe(
	decision downloadWindowDecision,
	windowCap int32,
) downloadWindowDecision {
	step := downloadWindowGrowthStep(decision.oldWindow, c.steady)
	newWindow := decision.oldWindow + step
	if windowCap > 0 && newWindow > windowCap {
		newWindow = windowCap
	}
	if newWindow <= decision.oldWindow {
		return decision
	}

	c.probe = downloadWindowProbe{
		active:     true,
		warmup:     !c.steady,
		fromWindow: decision.oldWindow,
		goodput:    decision.epoch.goodput(),
		p90Ms:      decision.epoch.p90Ms,
		lossRate:   decision.epoch.failureRate(),
	}
	decision.newWindow = newWindow
	if c.steady {
		decision.reason = "probe"
	} else {
		decision.reason = "warmup"
	}
	return decision
}

func (c *downloadWindowController) evaluateProbe(
	decision downloadWindowDecision,
	windowCap int32,
) downloadWindowDecision {
	epoch := decision.epoch
	baseGoodput := c.probe.goodput
	goodputGain := 0.0
	if baseGoodput > 0 {
		goodputGain = epoch.goodput()/baseGoodput - 1
	}
	latencyOK := !latencyRegressed(epoch.p90Ms, c.probe.p90Ms)
	lossOK := !lossRegressed(epoch.failureRate(), c.probe.lossRate)
	if goodputGain >= downloadWindowProbeMinGain && latencyOK && lossOK {
		c.setReference(epoch, decision.oldWindow)
		c.probe = downloadWindowProbe{}
		if windowCap > 0 && decision.oldWindow >= windowCap {
			return decision
		}
		return c.startProbe(decision, windowCap)
	}

	c.probe.epochs++
	if c.probe.epochs < downloadWindowProbeEpochs {
		return decision
	}

	decision.newWindow = c.bestWindow
	if decision.newWindow < 1 || decision.newWindow > decision.oldWindow {
		decision.newWindow = c.probe.fromWindow
	}
	decision.reason = "probe_no_gain"
	c.steady = true
	c.probe = downloadWindowProbe{}
	c.badEpochs = 0
	c.cooldown = downloadWindowCooldownEpochs
	return decision
}

func downloadWindowGrowthStep(window int32, steady bool) int32 {
	if !steady {
		return max(int32(1), window/2)
	}
	step := max(int32(1), window/downloadWindowSteadyGrowthDiv)
	return min(step, downloadWindowSteadyGrowthCap)
}

func decreasedDownloadWindow(window int32) int32 {
	decrease := max(int32(1), window/downloadWindowDecreaseDiv)
	return max(int32(1), window-decrease)
}

func latencyRegressed(currentMs, baselineMs int64) bool {
	if currentMs <= 0 || baselineMs <= 0 {
		return false
	}

	allowedRise := baselineMs * downloadWindowLatencyRisePercent / 100
	if allowedRise < downloadWindowLatencyRiseFloorMs {
		allowedRise = downloadWindowLatencyRiseFloorMs
	}
	return currentMs > baselineMs+allowedRise
}

func severeLatencyRegressed(currentMs, baselineMs int64) bool {
	if currentMs <= 0 || baselineMs <= 0 {
		return false
	}
	return currentMs >= baselineMs*downloadWindowSevereLatencyRatio
}

func lossRegressed(current, baseline float64) bool {
	allowedRise := baseline * 0.5
	if allowedRise < downloadWindowLossRiseFloor {
		allowedRise = downloadWindowLossRiseFloor
	}
	return current > baseline+allowedRise
}

func (c *downloadWindowController) setReference(epoch downloadWindowEpoch, window int32) {
	c.reference = epoch
	c.bestWindow = window
}

func (c *downloadWindowController) updateReference(epoch downloadWindowEpoch, window int32) {
	if c.reference.successes == 0 {
		c.setReference(epoch, window)
		return
	}

	goodput := 0.75*c.reference.goodput() + 0.25*epoch.goodput()
	lossRate := 0.75*c.reference.failureRate() + 0.25*epoch.failureRate()
	c.reference = downloadWindowEpoch{
		duration:    time.Second,
		successes:   1,
		bytes:       uint64(goodput),
		p50Ms:       (3*c.reference.p50Ms + epoch.p50Ms) / 4,
		p90Ms:       (3*c.reference.p90Ms + epoch.p90Ms) / 4,
		lossRate:    lossRate,
		lossRateSet: true,
	}
	c.bestWindow = window
}

func (c *downloadWindowController) afterDecrease() {
	c.steady = true
	c.probe = downloadWindowProbe{}
	c.badEpochs = 0
	c.cooldown = downloadWindowCooldownEpochs
}

func (c *downloadWindowController) resetAdaptation() {
	c.reference = downloadWindowEpoch{}
	c.bestWindow = 0
	c.probe = downloadWindowProbe{}
	c.badEpochs = 0
	c.cooldown = 0
}

func (c *PeerConnection) observeDownloadWindow(
	sample downloadWindowSample,
) (downloadWindowDecision, bool) {
	c.downloadWindowMx.Lock()
	defer c.downloadWindowMx.Unlock()
	sample.at = time.Now()

	window := c.MaxInflightPieces.Load()
	decision, epochReady := c.downloadWindow.observe(
		sample,
		window,
		dataQueueInflightCap(c),
	)
	if !epochReady || !decision.changed() {
		return decision, false
	}

	if !c.MaxInflightPieces.CompareAndSwap(decision.oldWindow, decision.newWindow) {
		c.downloadWindow.resetAdaptation()
		return downloadWindowDecision{}, false
	}

	c.LastChange.Store(sample.at.UnixMilli())
	c.StableCount.Store(0)
	c.UnstableCount.Store(0)
	if decision.newWindow > decision.oldWindow {
		c.UpStreak.Add(1)
		c.DownStreak.Store(0)
	} else {
		c.DownStreak.Add(1)
		c.UpStreak.Store(0)
	}

	return decision, true
}

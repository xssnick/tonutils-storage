package storage

import (
	"encoding/hex"
	"math"
	"sync/atomic"
	"time"
)

type PeerInfo struct {
	Addr       string
	Uploaded   uint64
	Downloaded uint64

	peer          *storagePeer
	uploadSpeed   *speedInfo
	downloadSpeed *speedInfo
}

const (
	peerIdleTimeout       = 10 * time.Minute
	peerIdleSweepInterval = 30 * time.Second
)

var (
	UploadStatsFlushBytes    = uint64(16 << 20)
	UploadStatsFlushInterval = 10 * time.Second
)

func (t *Torrent) GetPeers() map[string]PeerInfo {
	t.peersMx.RLock()
	defer t.peersMx.RUnlock()

	peers := make(map[string]PeerInfo, len(t.peers))
	for s, info := range t.peers {
		peers[s] = *info
	}
	return peers
}

func (t *Torrent) TouchPeer(peer *storagePeer) *PeerInfo {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	return t.touchPeer(peer)
}

func (t *Torrent) UpdateDownloadedPeer(peer *storagePeer, bytes uint64) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(peer)
	p.Downloaded += bytes
}

func (t *Torrent) RemovePeer(id []byte) {
	strId := hex.EncodeToString(id)

	t.peersMx.Lock()
	delete(t.peers, strId)
	t.peersMx.Unlock()

	t.compactNewPieces()
	t.wake.fire()
}

func (t *Torrent) GetPeer(id []byte) *PeerInfo {
	t.peersMx.RLock()
	defer t.peersMx.RUnlock()

	peer := t.peers[hex.EncodeToString(id)]
	if peer == nil {
		return nil
	}
	copy := *peer
	return &copy
}

func (t *Torrent) ResetDownloadPeer(id []byte) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	strId := hex.EncodeToString(id)
	p := t.peers[strId]
	if p != nil {
		p.Downloaded = 0
		p.downloadSpeed = &speedInfo{}
	}
}

func (t *Torrent) UpdateUploadedPeer(peer *storagePeer, bytes uint64) {
	uploaded := atomic.AddUint64(&t.stats.Uploaded, bytes)
	t.maybeStoreUploadStats(uploaded, false)

	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(peer)
	p.Uploaded += bytes
}

func (t *Torrent) maybeStoreUploadStats(uploaded uint64, force bool) {
	if t.db == nil {
		return
	}

	now := time.Now()
	stored := atomic.LoadUint64(&t.stats.StoredUploaded)
	lastAtMs := atomic.LoadInt64(&t.stats.LastUploadStatsStoredAtMs)
	if force && uploaded == stored && lastAtMs != 0 {
		return
	}

	if !force {
		bytesDue := UploadStatsFlushBytes == 0 || uploaded-stored >= UploadStatsFlushBytes
		timeDue := lastAtMs == 0 || now.UnixMilli()-lastAtMs >= UploadStatsFlushInterval.Milliseconds()
		if !bytesDue && !timeDue {
			return
		}
	}

	if !atomic.CompareAndSwapUint64(&t.stats.StoredUploaded, stored, uploaded) {
		return
	}
	atomic.StoreInt64(&t.stats.LastUploadStatsStoredAtMs, now.UnixMilli())
	_ = t.db.UpdateUploadStats(t.BagID, uploaded)
}

func (t *Torrent) flushUploadStats() {
	t.maybeStoreUploadStats(atomic.LoadUint64(&t.stats.Uploaded), true)
}

func (p *storagePeer) markActivity() {
	atomic.StoreInt64(&p.lastActivityAt, time.Now().UnixMilli())
}

func (p *storagePeer) lastActivity() time.Time {
	ts := atomic.LoadInt64(&p.lastActivityAt)
	if ts == 0 {
		ts = atomic.LoadInt64(&p.session.sessionInitAt)
	}
	if ts == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ts)
}

func (p *storagePeer) isIdle(now time.Time, maxIdle time.Duration) bool {
	last := p.lastActivity()
	if last.IsZero() {
		return false
	}
	return now.Sub(last) > maxIdle
}

func (t *Torrent) touchPeer(peer *storagePeer) *PeerInfo {
	peer.markActivity()

	strId := hex.EncodeToString(peer.nodeId)
	p := t.peers[strId]
	if p == nil {
		p = &PeerInfo{
			uploadSpeed:   &speedInfo{},
			downloadSpeed: &speedInfo{},
		}
		p.peer = peer
		p.Addr = peer.nodeAddr

		t.peers[strId] = p
	} else {
		p.peer = peer
		p.Addr = peer.nodeAddr
	}
	t.wake.fire()

	return p
}

func (p *PeerInfo) GetDownloadSpeed() uint64 {
	return uint64(p.downloadSpeed.display())
}

func (p *PeerInfo) GetUploadSpeed() uint64 {
	return uint64(p.uploadSpeed.display())
}

type speedInfo struct {
	wantReset bool

	prevBytes uint64
	lastTime  time.Time
	lastRise  time.Time

	speed float64
	init  bool

	dispSpeed     float64
	dispSpeedBits atomic.Uint64
}

const (
	speedRiseAlpha        = 0.45
	speedFallAlpha        = 0.18
	speedIdleAlpha        = 0.08
	speedDisplayRiseAlpha = 0.55
	speedDisplayFallAlpha = 0.22
	speedHoldNoRiseFor    = 2 * time.Second
)

func (s *speedInfo) calculate(nowBytes uint64) float64 {
	return s.calculateAt(nowBytes, time.Now())
}

func speedAlpha(base, dtSeconds float64) float64 {
	if base <= 0 || dtSeconds <= 0 {
		return 0
	}
	if base >= 1 {
		return 1
	}
	if dtSeconds > 10 {
		dtSeconds = 10
	}
	return 1 - math.Pow(1-base, dtSeconds)
}

func (s *speedInfo) calculateAt(nowBytes uint64, now time.Time) float64 {
	if !s.init {
		s.prevBytes = nowBytes
		s.lastTime = now
		s.init = true
		s.dispSpeed = 0
		s.publishDisplay()
		return s.dispSpeed
	}

	if nowBytes < s.prevBytes {
		s.prevBytes = nowBytes
		s.lastTime = now
		s.lastRise = time.Time{}
		s.speed = 0
		s.dispSpeed = 0
		s.publishDisplay()
		return s.dispSpeed
	}

	dt := now.Sub(s.lastTime).Seconds()
	if dt > 0 {
		deltaBytes := nowBytes - s.prevBytes
		instant := float64(deltaBytes) / dt

		sampleAlpha := speedFallAlpha
		switch {
		case deltaBytes == 0 && !s.lastRise.IsZero() && now.Sub(s.lastRise) <= speedHoldNoRiseFor:
			instant = s.speed
		case deltaBytes == 0:
			sampleAlpha = speedIdleAlpha
		case instant >= s.speed:
			sampleAlpha = speedRiseAlpha
			s.lastRise = now
		default:
			s.lastRise = now
		}

		s.speed += (instant - s.speed) * speedAlpha(sampleAlpha, dt)

		displayAlpha := speedDisplayFallAlpha
		if s.speed >= s.dispSpeed {
			displayAlpha = speedDisplayRiseAlpha
		}
		s.dispSpeed += (s.speed - s.dispSpeed) * speedAlpha(displayAlpha, dt)
		if s.dispSpeed < 1 {
			s.dispSpeed = 0
		}
	}

	s.prevBytes = nowBytes
	s.lastTime = now
	s.publishDisplay()

	return s.dispSpeed
}

func (s *speedInfo) display() float64 {
	return math.Float64frombits(s.dispSpeedBits.Load())
}

func (s *speedInfo) publishDisplay() {
	s.dispSpeedBits.Store(math.Float64bits(s.dispSpeed))
}

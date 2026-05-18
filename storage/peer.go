package storage

import (
	"encoding/hex"
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
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	return t.peers[hex.EncodeToString(id)]
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
	return uint64(p.downloadSpeed.dispSpeed)
}

func (p *PeerInfo) GetUploadSpeed() uint64 {
	return uint64(p.uploadSpeed.dispSpeed)
}

type speedInfo struct {
	wantReset bool

	prevBytes uint64
	lastTime  time.Time

	speed float64
	init  bool

	dispSpeed float64
}

func (s *speedInfo) calculate(nowBytes uint64) float64 {
	now := time.Now()

	if !s.init {
		s.prevBytes = nowBytes
		s.lastTime = now
		s.init = true
		s.dispSpeed = 0
		return s.dispSpeed
	}

	dt := now.Sub(s.lastTime).Seconds()
	if dt > 0 {
		const alpha = 0.05
		delta := float64(nowBytes - s.prevBytes)
		instant := delta / dt
		s.speed = alpha*instant + (1-alpha)*s.speed
	}

	s.prevBytes = nowBytes
	s.lastTime = now

	// smooth animation
	const beta = 0.1
	s.dispSpeed += (s.speed - s.dispSpeed) * beta

	return s.dispSpeed
}

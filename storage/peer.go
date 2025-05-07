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
	defer t.peersMx.Unlock()

	delete(t.peers, strId)
	delete(t.knownNodes, strId)
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
	_ = t.db.UpdateUploadStats(t.BagID, atomic.AddUint64(&t.stats.Uploaded, bytes))

	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(peer)
	p.Uploaded += bytes
}

func (t *Torrent) touchPeer(peer *storagePeer) *PeerInfo {
	strId := hex.EncodeToString(peer.nodeId)
	p := t.peers[strId]
	if p == nil {
		p = &PeerInfo{
			uploadSpeed:   &speedInfo{},
			downloadSpeed: &speedInfo{},
		}
		t.peers[strId] = p
	}
	p.peer = peer
	p.Addr = peer.nodeAddr
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

package storage

import (
	"encoding/hex"
	"sync/atomic"
	"time"
)

type speedInfo struct {
	wantReset bool
	prevBytes uint64
	buf       []uint64
	off       uint64
	speed     uint64
}

type PeerInfo struct {
	Addr       string
	LastSeenAt time.Time
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
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	strId := hex.EncodeToString(id)
	delete(t.peers, strId)
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
		p.downloadSpeed = &speedInfo{
			buf: make([]uint64, 200),
		}
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
			uploadSpeed: &speedInfo{
				buf: make([]uint64, 200),
			},
			downloadSpeed: &speedInfo{
				buf: make([]uint64, 200),
			},
		}

		if peer.globalCtx.Err() == nil {
			// add only if it is alive
			t.peers[strId] = p
		}
	}
	p.peer = peer
	p.Addr = peer.nodeAddr
	p.LastSeenAt = time.Now()
	return p
}

func (t *Torrent) runPeersMonitor() {
	for {
		select {
		case <-t.globalCtx.Done():
			t.peersMx.Lock()
			t.peers = map[string]*PeerInfo{}
			t.peersMx.Unlock()
			return
		case <-time.After(100 * time.Millisecond):
		}

		t.peersMx.Lock()
		for k, p := range t.peers {
			if p.LastSeenAt.Add(5 * time.Minute).Before(time.Now()) {
				delete(t.peers, k)
			}
		}
		t.peersMx.Unlock()

		for _, p := range t.GetPeers() {
			p.downloadSpeed.calculate(p.Downloaded, 10)
			p.uploadSpeed.calculate(p.Uploaded, 10)
		}
	}
}

func (s *speedInfo) calculate(nowBytes uint64, amp uint64) {
	s.buf[s.off%uint64(len(s.buf))] = nowBytes - s.prevBytes
	s.off++

	m := uint64(len(s.buf))
	if m > s.off {
		m = s.off
	}

	sum := uint64(0)
	for i := uint64(0); i < m; i++ {
		sum += s.buf[i]
	}
	s.speed = (sum / m) * amp
	s.prevBytes = nowBytes
}

func (p *PeerInfo) GetDownloadSpeed() uint64 {
	return p.downloadSpeed.speed
}

func (p *PeerInfo) GetUploadSpeed() uint64 {
	return p.uploadSpeed.speed
}

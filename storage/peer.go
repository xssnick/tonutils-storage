package storage

import (
	"encoding/hex"
	"time"
)

type speedInfo struct {
	prevAt    time.Time
	prevBytes uint64
	nextBytes uint64
	speed     uint64
}

type PeerInfo struct {
	LastSeenAt time.Time
	Uploaded   uint64
	Downloaded uint64

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

func (t *Torrent) TouchPeer(id []byte) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	t.touchPeer(id)
}

func (t *Torrent) UpdateDownloadedPeer(id []byte, bytes uint64) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(id)
	p.Downloaded += bytes
}

func (t *Torrent) UpdateUploadedPeer(id []byte, bytes uint64) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(id)
	p.Uploaded += bytes
}

func (t *Torrent) touchPeer(id []byte) *PeerInfo {
	strId := hex.EncodeToString(id)
	p := t.peers[strId]
	if p == nil {
		p = &PeerInfo{
			uploadSpeed:   &speedInfo{},
			downloadSpeed: &speedInfo{},
		}
		t.peers[strId] = p
	}
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
			p.downloadSpeed.calculate(p.Downloaded)
			p.uploadSpeed.calculate(p.Uploaded)
		}
	}
}

func (s *speedInfo) calculate(nowBytes uint64) {
	downloaded := nowBytes - s.prevBytes

	period := uint64(time.Since(s.prevAt) / (1 * time.Second))
	if period == 0 {
		period = 1
	}

	if time.Since(s.prevAt) > 20*time.Second {
		s.prevBytes = s.nextBytes
		s.prevAt = time.Now()
	} else if time.Since(s.prevAt) >= 10*time.Second {
		s.nextBytes = nowBytes
	}

	s.speed = downloaded / period
}

func (p *PeerInfo) GetDownloadSpeed() uint64 {
	return p.downloadSpeed.speed
}

func (p *PeerInfo) GetUploadSpeed() uint64 {
	return p.uploadSpeed.speed
}

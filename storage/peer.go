package storage

import (
	"encoding/hex"
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

func (t *Torrent) TouchPeer(id []byte, addr string) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	t.touchPeer(id, addr)
}

func (t *Torrent) UpdateDownloadedPeer(id []byte, addr string, bytes uint64) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(id, addr)
	p.Downloaded += bytes
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

func (t *Torrent) UpdateUploadedPeer(id []byte, addr string, bytes uint64) {
	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	p := t.touchPeer(id, addr)
	p.Uploaded += bytes
}

func (t *Torrent) touchPeer(id []byte, addr string) *PeerInfo {
	strId := hex.EncodeToString(id)
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
		t.peers[strId] = p
	}
	p.Addr = addr
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

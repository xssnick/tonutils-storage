package storage

import (
	"encoding/hex"
	"time"
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
		p = &PeerInfo{}
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
		case <-time.After(250 * time.Millisecond):
		}

		t.peersMx.Lock()
		for k, p := range t.peers {
			if p.LastSeenAt.Add(5 * time.Minute).Before(time.Now()) {
				delete(t.peers, k)
			}
		}
		t.peersMx.Unlock()

	}
}

package storage

import (
	"encoding/hex"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"sync"
)

type PeerConnection struct {
	rldp overlay.RLDP
	adnl adnl.Peer

	mx         sync.RWMutex
	failedBags map[string]bool
	usedByBags map[string]*storagePeer
}

func (c *PeerConnection) FailedFor(peer *storagePeer, failed bool) {
	c.mx.Lock()
	defer c.mx.Unlock()
	if failed {
		c.failedBags[string(peer.torrent.BagID)] = true
	} else {
		delete(c.failedBags, string(peer.torrent.BagID))
	}

}
func (c *PeerConnection) CloseFor(peer *storagePeer) {
	c.mx.Lock()
	defer c.mx.Unlock()

	delete(c.usedByBags, string(peer.torrent.BagID))
	Logger("[STORAGE_PEER] CLOSING", hex.EncodeToString(c.adnl.GetID()), "FOR", hex.EncodeToString(peer.torrent.BagID), "LEFT USAGES", len(c.usedByBags))
	for bagID, otherBagPeer := range c.usedByBags {
		_, hasFailsForBag := c.failedBags[bagID]
		if hasFailsForBag {
			delete(c.usedByBags, string(bagID))
			Logger("[STORAGE_PEER] CLOSING", hex.EncodeToString(otherBagPeer.nodeId), "FOR", hex.EncodeToString([]byte(bagID)), "DUE TO FAILURES")
		}
	}
	if len(c.usedByBags) == 0 {
		Logger("[STORAGE_PEER] DISCONNECTING, NO MORE USAGES FOR", hex.EncodeToString(c.adnl.GetID()))
		c.adnl.Close()
	}
}

func (c *PeerConnection) UseFor(peer *storagePeer) {
	c.mx.Lock()
	defer c.mx.Unlock()

	Logger("[STORAGE_PEER] USING", hex.EncodeToString(c.adnl.GetID()), "FOR", hex.EncodeToString(peer.torrent.BagID))

	c.usedByBags[string(peer.torrent.BagID)] = peer
}

func (c *PeerConnection) GetFor(id []byte) *storagePeer {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.usedByBags[string(id)]
}

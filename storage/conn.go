package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"strings"
	"sync"
)

type PeerConnection struct {
	rldp           overlay.RLDP
	adnl           adnl.Peer
	rldpQueue      chan struct{}
	bagsInitQueue  chan struct{}
	inflightPieces int32

	mx         sync.RWMutex
	failedBags map[string]bool
	usedByBags map[string]*storagePeer
	connClosed bool
}

func (c *PeerConnection) FailedFor(peer *storagePeer, err error, ping bool) {
	c.mx.Lock()
	defer c.mx.Unlock()
	if err != nil {
		if ping || strings.Contains(err.Error(), "connection was closed") {
			c.connClosed = true
		}
		c.failedBags[string(peer.torrent.BagID)] = true
	} else {
		delete(c.failedBags, string(peer.torrent.BagID))
	}

}
func (c *PeerConnection) CloseFor(peer *storagePeer) {
	c.mx.Lock()
	defer c.mx.Unlock()

	delete(c.usedByBags, string(peer.torrent.BagID))
	Logger("[STORAGE_PEER] CLOSING", hex.EncodeToString(c.adnl.GetID()), "FOR", hex.EncodeToString(peer.torrent.BagID), "LEFT USAGES", len(c.usedByBags), "FORCE", c.connClosed)
	if !c.connClosed {
		for bagID, otherBagPeer := range c.usedByBags {
			_, hasFailsForBag := c.failedBags[bagID]
			if hasFailsForBag {
				delete(c.usedByBags, string(bagID))
				Logger("[STORAGE_PEER] CLOSING", hex.EncodeToString(otherBagPeer.nodeId), "FOR", hex.EncodeToString([]byte(bagID)), "DUE TO FAILURES")
			}
		}
	}
	if c.connClosed || len(c.usedByBags) == 0 {
		Logger("[STORAGE_PEER] DISCONNECTING, NO MORE USAGES FOR", hex.EncodeToString(c.adnl.GetID()))
		c.adnl.Close()
	} else {
		Logger("[STORAGE_PEER] FOR", hex.EncodeToString(c.adnl.GetID()), len(c.usedByBags), " more usages")
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

var ErrQueueIsBusy = errors.New("queue is busy")

func (c *PeerConnection) AcquireQueueSlotWait(ctx context.Context) error {
	select {
	case c.rldpQueue <- struct{}{}:
	case <-ctx.Done():
		return ErrQueueIsBusy
	}
	return nil
}

func (c *PeerConnection) AcquireQueueSlot() error {
	select {
	case c.rldpQueue <- struct{}{}:
	default:
		return ErrQueueIsBusy
	}
	return nil
}

func (c *PeerConnection) FreeQueueSlot() {
	<-c.rldpQueue
}

package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"sync"
)

type PeerConnection struct {
	rldp           overlay.RLDP
	adnl           adnl.Peer
	rldpQueue      chan struct{}
	bagsInitQueue  chan struct{}
	inflightPieces int32

	mx         sync.RWMutex
	usedByBags map[string]*storagePeer
}

func (c *PeerConnection) CloseFor(peer *storagePeer) {
	c.mx.Lock()
	defer c.mx.Unlock()

	delete(c.usedByBags, string(peer.torrent.BagID))
	Logger("[STORAGE_PEER] CLOSING", hex.EncodeToString(c.adnl.GetID()), "FOR", hex.EncodeToString(peer.torrent.BagID), "LEFT USAGES", len(c.usedByBags))

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

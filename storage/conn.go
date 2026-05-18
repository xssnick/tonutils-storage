package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"sync"
	"sync/atomic"
)

type PeerConnection struct {
	node             *overlay.Node
	rldp             overlay.RLDP
	adnl             adnl.Peer
	srv              *Server
	controlQueue     chan struct{}
	initControlQueue chan struct{}
	dataQueue        chan struct{}
	bagsInitQueue    chan struct{}

	InflightPieces    atomic.Int32
	MaxInflightPieces atomic.Int32

	StableCount   atomic.Int64
	UnstableCount atomic.Int64
	IsLastSuccess atomic.Bool
	LastChange    atomic.Int64
	UpStreak      atomic.Int64
	DownStreak    atomic.Int64

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

func (c *PeerConnection) initQueue() chan struct{} {
	if c.initControlQueue == nil {
		return c.controlQueue
	}
	return c.initControlQueue
}

func acquireQueueSlot(ctx context.Context, queue chan struct{}, wait bool) error {
	if wait {
		select {
		case queue <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ErrQueueIsBusy
		}
	}

	select {
	case queue <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ErrQueueIsBusy
	default:
		return ErrQueueIsBusy
	}
}

func withQueueSlot(ctx context.Context, queue chan struct{}, wait bool, run func() error) error {
	if err := acquireQueueSlot(ctx, queue, wait); err != nil {
		return err
	}
	defer func() { <-queue }()

	return run()
}

func (c *PeerConnection) withControlQueueSlot(ctx context.Context, wait bool, run func() error) error {
	return withQueueSlot(ctx, c.controlQueue, wait, run)
}

func (c *PeerConnection) withInitControlQueueSlot(ctx context.Context, wait bool, run func() error) error {
	return withQueueSlot(ctx, c.initQueue(), wait, run)
}

func (c *PeerConnection) withDataQueueSlot(ctx context.Context, wait bool, run func() error) error {
	return withQueueSlot(ctx, c.dataQueue, wait, run)
}

package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	uploadGlobalMemoryLimit = 256 << 20
	uploadPeerMemoryLimit   = 64 << 20
	uploadSweepInterval     = 100 * time.Millisecond
)

type uploadMemoryBudget struct {
	mx   sync.Mutex
	used uint64
}

func (b *uploadMemoryBudget) reserve(size uint64) bool {
	b.mx.Lock()
	defer b.mx.Unlock()
	if size > uploadGlobalMemoryLimit || b.used > uploadGlobalMemoryLimit-size {
		return false
	}
	b.used += size
	return true
}

func (b *uploadMemoryBudget) release(size uint64) {
	b.mx.Lock()
	b.used -= size
	b.mx.Unlock()
}

type uploadLease struct {
	conn        *PeerConnection
	key         string
	size        uint64
	deadline    time.Time
	currentPart uint32
	lastPart    uint32
	started     bool
	retained    bool
	onComplete  func()
	once        sync.Once
	ackMx       sync.Mutex
}

func reverseUploadTransferID(transfer []byte) string {
	id := make([]byte, 32)
	copy(id, transfer)
	for i := range id {
		id[i] ^= 0xff
	}
	return string(id)
}

func uploadTransferDeadline(ctx context.Context, queryTimeout uint32) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(15 * time.Second)
	}
	seconds := deadline.Unix()
	if int64(queryTimeout) < seconds {
		seconds = int64(queryTimeout)
	}
	if minimum := time.Now().Unix() + 1; seconds < minimum {
		seconds = minimum
	}
	return time.Unix(seconds, 0)
}

func (c *PeerConnection) reserveUpload(transfer []byte, pieceSize uint64, deadline time.Time) (*uploadLease, error) {
	if pieceSize > uploadPeerMemoryLimit/4 {
		return nil, fmt.Errorf("piece exceeds upload memory budget")
	}
	// Account for the piece, TL serialization and FEC working storage before
	// loading from disk. Proofs for supported bags fit inside the extra 64 KiB.
	size := pieceSize*4 + (64 << 10)
	c.uploadMx.Lock()
	defer c.uploadMx.Unlock()
	key := reverseUploadTransferID(transfer)
	if c.uploads[key] != nil {
		return nil, fmt.Errorf("upload transfer is already active")
	}
	if size > uploadPeerMemoryLimit || c.uploadBytes > uploadPeerMemoryLimit-size {
		return nil, fmt.Errorf("upload queue is busy")
	}
	if c.srv != nil && !c.srv.uploadBudget.reserve(size) {
		return nil, fmt.Errorf("upload queue is busy")
	}
	lease := &uploadLease{conn: c, key: key, size: size, deadline: deadline}
	if c.uploads == nil {
		c.uploads = make(map[string]*uploadLease)
	}
	c.uploads[key] = lease
	c.uploadBytes += size
	return lease, nil
}

func (l *uploadLease) setOnComplete(callback func()) {
	l.conn.uploadMx.Lock()
	l.onComplete = callback
	l.conn.uploadMx.Unlock()
}

func (l *uploadLease) retainUntil(deadline time.Time) {
	c := l.conn
	c.uploadMx.Lock()
	if deadline.After(l.deadline) {
		l.deadline = deadline
	}
	l.retained = true
	start := c.uploads[l.key] == l && !c.uploadSweepRunning
	if start {
		c.uploadSweepRunning = true
	}
	c.uploadMx.Unlock()
	if start {
		go c.sweepUploads()
	}
}

func (l *uploadLease) release(completed bool) {
	l.once.Do(func() {
		c := l.conn
		c.uploadMx.Lock()
		delete(c.uploads, l.key)
		c.uploadBytes -= l.size
		callback := l.onComplete
		c.uploadMx.Unlock()
		if c.srv != nil {
			c.srv.uploadBudget.release(l.size)
		}
		if completed && callback != nil {
			callback()
		}
	})
}

func (c *PeerConnection) noteUploadPart(data tl.Serializable) {
	var part rldp.MessagePart
	switch message := data.(type) {
	case rldp.MessagePart:
		part = message
	case rldp.MessagePartV2:
		part = rldp.MessagePart(message)
	default:
		return
	}
	c.uploadMx.Lock()
	if lease := c.uploads[string(part.TransferID)]; lease != nil && part.TotalSize > 0 && rldp.PartSize > 0 && (!lease.started || part.Part >= lease.currentPart) {
		lease.started = true
		lease.currentPart = part.Part
		lease.lastPart = uint32((part.TotalSize - 1) / uint64(rldp.PartSize))
	}
	c.uploadMx.Unlock()
}

func (c *PeerConnection) completeUploadPart(id []byte, part uint32) {
	c.uploadMx.Lock()
	lease := c.uploads[string(id)]
	complete := lease != nil && lease.started && lease.currentPart == part && lease.lastPart == part
	if lease != nil && lease.started && lease.currentPart == part && part < lease.lastPart {
		// RLDP advances to the next part on this ACK even when its rate bucket
		// allows zero initial symbols, so the outgoing hook may not run yet.
		lease.currentPart++
	}
	c.uploadMx.Unlock()
	if complete {
		lease.release(true)
	}
}

func (c *PeerConnection) releaseUploads() {
	c.uploadMx.Lock()
	leases := make([]*uploadLease, 0, len(c.uploads))
	for _, lease := range c.uploads {
		leases = append(leases, lease)
	}
	c.uploadMx.Unlock()
	for _, lease := range leases {
		lease.release(false)
	}
}

func (c *PeerConnection) expireUploads(now time.Time) {
	// Piece answers have their own sender. Its idle state proves expired
	// answer buffers have been retired, independent of ongoing download or
	// control queries on the primary transport.
	if c.uploadSenderStats().Active.OutboundTransfers != 0 {
		return
	}
	c.uploadMx.Lock()
	var expired []*uploadLease
	for _, lease := range c.uploads {
		if lease.retained && !now.Before(lease.deadline) {
			expired = append(expired, lease)
		}
	}
	c.uploadMx.Unlock()
	for _, lease := range expired {
		lease.release(false)
	}
}

func (c *PeerConnection) sweepUploads() {
	ticker := time.NewTicker(uploadSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.adnl.GetCloserCtx().Done():
			c.rldp.Close()
			c.closeUploadSender()
			c.releaseUploads()
		case now := <-ticker.C:
			c.expireUploads(now)
		}
		c.uploadMx.Lock()
		if len(c.uploads) == 0 {
			c.uploadSweepRunning = false
			c.uploadMx.Unlock()
			return
		}
		c.uploadMx.Unlock()
	}
}

// Track storage answer lifetime below the overlay wrapper, preserving the
// wrapper's concrete type and its normal query and broadcast handling.
type uploadTrackingPeer struct {
	adnl.Peer
	conn *PeerConnection
}

func (p *uploadTrackingPeer) SendCustomMessage(ctx context.Context, data tl.Serializable) error {
	p.conn.noteUploadPart(data)
	return p.Peer.SendCustomMessage(ctx, data)
}

func (p *uploadTrackingPeer) SetCustomMessageHandler(handler func(*adnl.MessageCustom) error) {
	if handler == nil {
		p.Peer.SetCustomMessageHandler(nil)
		return
	}
	p.Peer.SetCustomMessageHandler(func(message *adnl.MessageCustom) error {
		var completion *rldp.Complete
		switch data := message.Data.(type) {
		case rldp.Complete:
			completion = &data
		case rldp.CompleteV2:
			converted := rldp.Complete(data)
			completion = &converted
		}
		if completion != nil {
			p.conn.uploadMx.Lock()
			lease := p.conn.uploads[string(completion.TransferID)]
			p.conn.uploadMx.Unlock()
			if lease != nil {
				// Different ADNL listener threads may handle successive ACKs.
				// Keep accounting in the same order as transport part advancement.
				lease.ackMx.Lock()
				defer lease.ackMx.Unlock()
			}
		}
		// The primary handler can normalize message.Data in place. Preserve
		// the wire control type for independent V1/V2 sender negotiation.
		bulkMessage := *message
		if err := handler(message); err != nil {
			return err
		}
		if err := p.conn.dispatchUploadControl(&bulkMessage); err != nil {
			return err
		}
		if completion != nil {
			p.conn.completeUploadPart(completion.TransferID, completion.Part)
		}
		return nil
	})
}

type uploadControlHandler func(*adnl.MessageCustom) error

// Receive only transport confirmations for the bulk sender. All queries and
// answer payloads continue through the primary RLDP decoder exactly once.
type uploadControlADNL struct {
	rldp.ADNL
	handler atomic.Pointer[uploadControlHandler]
}

func (a *uploadControlADNL) SetCustomMessageHandler(handler func(*adnl.MessageCustom) error) {
	if handler == nil {
		a.handler.Store(nil)
		return
	}
	callback := uploadControlHandler(handler)
	a.handler.Store(&callback)
}

func (c *PeerConnection) getUploadSender() (*rldp.RLDP, error) {
	c.uploadSenderMx.Lock()
	defer c.uploadSenderMx.Unlock()
	if err := c.adnl.GetCloserCtx().Err(); err != nil {
		return nil, err
	}
	if sender := c.uploadSender.Load(); sender != nil {
		return sender, nil
	}
	control := &uploadControlADNL{ADNL: c.rldp.GetADNL()}
	var sender *rldp.RLDP
	if c.rldp.Stats().Version == rldp.Version1 {
		sender = rldp.NewClient(control)
	} else {
		sender = rldp.NewClientV2(control)
	}
	c.uploadSender.Store(sender)
	return sender, nil
}

func (c *PeerConnection) dispatchUploadControl(message *adnl.MessageCustom) error {
	switch message.Data.(type) {
	case rldp.Complete, rldp.CompleteV2, rldp.Confirm, rldp.ConfirmV2:
	default:
		return nil
	}
	if sender := c.uploadSender.Load(); sender != nil {
		control := sender.GetADNL().(*uploadControlADNL)
		if callback := control.handler.Load(); callback != nil {
			return (*callback)(message)
		}
	}
	return nil
}

func (c *PeerConnection) uploadSenderStats() rldp.Stats {
	if sender := c.uploadSender.Load(); sender != nil {
		return sender.Stats()
	}
	return c.rldp.Stats()
}

func (c *PeerConnection) closeUploadSender() {
	c.uploadSenderMx.Lock()
	sender := c.uploadSender.Load()
	c.uploadSenderMx.Unlock()
	if sender != nil {
		sender.Close()
	}
}

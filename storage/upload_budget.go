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
	conn            *PeerConnection
	key             string
	size            uint64
	deadline        time.Time
	currentPart     uint32
	lastPart        uint32
	started         bool
	retained        bool
	sending         bool
	networkSends    int
	controlHandlers int
	finishPending   bool
	completed       bool
	sender          *uploadSender
	onComplete      func()
	once            sync.Once
	ackMx           sync.Mutex
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
	if c.uploadsClosed || (c.adnl != nil && c.adnl.GetCloserCtx().Err() != nil) {
		return nil, adnl.ErrPeerConnClosed
	}
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

// sendAnswer keeps the permit through serialization and the initial send burst,
// including a final ACK or disconnect arriving before SendAnswer returns.
func (l *uploadLease) sendAnswer(ctx context.Context, maxAnswerSize uint64, timeoutAt uint32, queryID, transfer []byte, answer tl.Serializable) error {
	c := l.conn
	sender, err := c.getUploadSender(l)
	if err != nil {
		return err
	}
	c.uploadMx.Lock()
	if c.uploadsClosed || c.uploads[l.key] != l || l.finishPending {
		c.uploadMx.Unlock()
		return adnl.ErrPeerConnClosed
	}
	if l.sending || l.retained {
		c.uploadMx.Unlock()
		return fmt.Errorf("upload transfer has already been sent")
	}
	l.sending = true
	c.uploadMx.Unlock()

	sendCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(sender.GetADNL().GetCloserCtx(), cancel)
	err = sender.SendAnswer(sendCtx, maxAnswerSize, timeoutAt, queryID, transfer, answer)
	stop()
	cancel()
	c.uploadMx.Lock()
	l.sending = false
	pending, completed := l.finishPending, l.completed
	c.uploadMx.Unlock()
	if err != nil || pending {
		l.release(completed)
	} else {
		l.retainUntil(uploadTransferDeadline(ctx, timeoutAt).Add(time.Second))
	}
	return err
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
	c := l.conn
	c.uploadMx.Lock()
	l.finishPending = true
	l.completed = l.completed || completed
	completed = l.completed
	if l.sending || l.networkSends != 0 || l.controlHandlers != 0 {
		c.uploadMx.Unlock()
		return
	}
	c.uploadMx.Unlock()
	l.once.Do(func() {
		// The isolated sender cannot contain another upload. Cancellation or
		// error can therefore retire this sender without closing shared ADNL.
		c.returnUploadSender(l)
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

func (c *PeerConnection) completeUploadPart(lease *uploadLease, part uint32, retired bool) {
	c.uploadMx.Lock()
	complete := !c.uploadsClosed && retired && lease.started && lease.currentPart == part && lease.lastPart == part
	if lease.started && lease.currentPart == part && part < lease.lastPart {
		// RLDP advances on this ACK even when its rate bucket allows no initial
		// symbols, so the outgoing hook may not have observed the next part.
		lease.currentPart++
	}
	c.uploadMx.Unlock()
	if retired {
		lease.release(complete)
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
	c.uploadMx.Lock()
	var candidates []*uploadLease
	for _, lease := range c.uploads {
		if lease.retained && !lease.sending {
			candidates = append(candidates, lease)
		}
	}
	c.uploadMx.Unlock()
	for _, lease := range candidates {
		// Serialize retirement with ACK accounting, so the sweeper cannot
		// consume a successful completion between the decoder and its callback.
		lease.ackMx.Lock()
		c.uploadMx.Lock()
		sender := lease.sender
		current := c.uploads[lease.key] == lease
		deadline := lease.deadline
		c.uploadMx.Unlock()
		if current && sender != nil && sender.transport.Stats().Active.OutboundTransfers == 0 {
			lease.release(false)
		} else if current && sender == nil && !now.Before(deadline) {
			lease.release(false)
		}
		lease.ackMx.Unlock()
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
			c.uploadMx.Lock()
			c.uploadSweepRunning = false
			c.uploadMx.Unlock()
			return
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

// Track answer lifetime below the overlay wrapper, retaining its normal query
// and broadcast handling. Payloads are decoded by the primary RLDP exactly once.
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
		id, completion := uploadControlTransfer(message.Data)
		p.conn.uploadMx.Lock()
		lease := p.conn.uploads[id]
		var sender *uploadSender
		if lease != nil {
			sender = lease.sender
		}
		p.conn.uploadMx.Unlock()
		if lease != nil {
			lease.ackMx.Lock()
			defer lease.ackMx.Unlock()
			// Retirement can return the sender to the idle pool while this
			// handler waits. Never deliver a delayed ACK to its next owner.
			p.conn.uploadMx.Lock()
			if p.conn.uploads[id] != lease || lease.sender != sender || sender == nil || sender.adnl.owner.Load() != lease {
				sender = nil
			} else {
				lease.controlHandlers++
				defer lease.finishControlHandler()
			}
			p.conn.uploadMx.Unlock()
		}
		// The primary handler can normalize Data in place. Preserve its wire
		// type for independent V1/V2 negotiation of the answer sender.
		bulkMessage := *message
		if err := handler(message); err != nil {
			return err
		}
		if sender != nil {
			wasActive := sender.transport.Stats().Active.OutboundTransfers != 0
			if callback := sender.adnl.handler.Load(); callback != nil {
				if err := (*callback)(&bulkMessage); err != nil {
					return err
				}
			}
			if completion != nil && wasActive {
				retired := sender.transport.Stats().Active.OutboundTransfers == 0
				p.conn.completeUploadPart(lease, completion.Part, retired)
			}
		}
		return nil
	})
}

// Keep ownership pinned from validation until both decoder callbacks finish.
// The sender may otherwise enter the idle pool while the primary decoder runs.
func (l *uploadLease) finishControlHandler() {
	c := l.conn
	c.uploadMx.Lock()
	l.controlHandlers--
	finish := l.finishPending && !l.sending && l.networkSends == 0 && l.controlHandlers == 0
	completed := l.completed
	c.uploadMx.Unlock()
	if finish {
		l.release(completed)
	}
}

func uploadControlTransfer(data tl.Serializable) (string, *rldp.Complete) {
	switch message := data.(type) {
	case rldp.Complete:
		return string(message.TransferID), &message
	case rldp.CompleteV2:
		complete := rldp.Complete(message)
		return string(message.TransferID), &complete
	case rldp.Confirm:
		return string(message.TransferID), nil
	case rldp.ConfirmV2:
		return string(message.TransferID), nil
	default:
		return "", nil
	}
}

type uploadControlHandler func(*adnl.MessageCustom) error

type uploadSender struct {
	transport *rldp.RLDP
	adnl      *uploadControlADNL
}

// Each sender owns exactly one answer at a time. Its private closer context
// retires only that transport; closing it must never close the physical ADNL.
type uploadControlADNL struct {
	rldp.ADNL
	handler atomic.Pointer[uploadControlHandler]
	owner   atomic.Pointer[uploadLease]
	ctx     context.Context
	cancel  context.CancelFunc
}

func (a *uploadControlADNL) GetCloserCtx() context.Context { return a.ctx }
func (a *uploadControlADNL) Close()                        { a.cancel() }

func (a *uploadControlADNL) SetCustomMessageHandler(handler func(*adnl.MessageCustom) error) {
	if handler == nil {
		a.handler.Store(nil)
		return
	}
	callback := uploadControlHandler(handler)
	a.handler.Store(&callback)
}

func (a *uploadControlADNL) SendCustomMessage(ctx context.Context, data tl.Serializable) error {
	owner := a.owner.Load()
	var id []byte
	switch part := data.(type) {
	case rldp.MessagePart:
		id = part.TransferID
	case rldp.MessagePartV2:
		id = part.TransferID
	}
	if owner == nil || string(id) != owner.key || a.ctx.Err() != nil {
		return adnl.ErrPeerConnClosed
	}
	c := owner.conn
	c.uploadMx.Lock()
	if a.owner.Load() != owner || c.uploads[owner.key] != owner || (owner.finishPending && !owner.sending) {
		c.uploadMx.Unlock()
		return adnl.ErrPeerConnClosed
	}
	owner.networkSends++
	c.uploadMx.Unlock()

	// Recovery already uses this private context. Multipart ACK handling
	// supplies Background, so use it there too; the initial burst is given a
	// context combining its caller's cancellation with this sender's close.
	if ctx.Done() == nil {
		ctx = a.ctx
	}
	err := a.ADNL.SendCustomMessage(ctx, data)
	c.uploadMx.Lock()
	owner.networkSends--
	finish := owner.finishPending && !owner.sending && owner.networkSends == 0 && owner.controlHandlers == 0
	completed := owner.completed
	c.uploadMx.Unlock()
	if finish {
		owner.release(completed)
	}
	return err
}

// Reuse one idle sender per peer, preserving its congestion history for serial
// uploads without retaining a goroutine pair for every previous concurrent lane.
// Concurrent answers have independent congestion controllers; the configured
// application upload limiter remains shared through Connector.ThrottleUpload.
// RLDP has no worker join: retired recovery-loop stack references disappear
// asynchronously. Owner checks prevent further sends, and active network calls
// must drain before returning the permit or reusing the idle sender.
func (c *PeerConnection) getUploadSender(lease *uploadLease) (*rldp.RLDP, error) {
	c.uploadSenderMx.Lock()
	defer c.uploadSenderMx.Unlock()
	c.uploadMx.Lock()
	defer c.uploadMx.Unlock()
	if c.uploadsClosed || c.uploads[lease.key] != lease || lease.finishPending || c.adnl.GetCloserCtx().Err() != nil {
		return nil, adnl.ErrPeerConnClosed
	}
	if lease.sender != nil {
		return lease.sender.transport, nil
	}
	sender := c.uploadIdleSender
	c.uploadIdleSender = nil
	version := c.rldp.Stats().Version
	if sender != nil {
		stats := sender.transport.Stats()
		if stats.Closed || stats.Version != version || sender.adnl.ctx.Err() != nil {
			sender.transport.Close()
			sender = nil
		}
	}
	if sender == nil {
		ctx, cancel := context.WithCancel(c.adnl.GetCloserCtx())
		control := &uploadControlADNL{ADNL: c.rldp.GetADNL(), ctx: ctx, cancel: cancel}
		sender = &uploadSender{adnl: control}
		if version == rldp.Version1 {
			sender.transport = rldp.NewClient(control)
		} else {
			sender.transport = rldp.NewClientV2(control)
		}
	}
	sender.adnl.owner.Store(lease)
	lease.sender = sender
	return sender.transport, nil
}

func (c *PeerConnection) returnUploadSender(lease *uploadLease) {
	c.uploadSenderMx.Lock()
	c.uploadMx.Lock()
	sender := lease.sender
	lease.sender = nil
	canPool := !c.uploadsClosed && c.adnl != nil && c.adnl.GetCloserCtx().Err() == nil && c.uploadIdleSender == nil
	c.uploadMx.Unlock()
	if sender != nil {
		sender.adnl.owner.Store(nil)
		if canPool && sender.transport.Stats().Active.OutboundTransfers == 0 && !sender.transport.Stats().Closed {
			c.uploadIdleSender = sender
		} else {
			sender.transport.Close()
		}
	}
	c.uploadSenderMx.Unlock()
}

func (c *PeerConnection) closeUploadSender() {
	c.uploadSenderMx.Lock()
	c.uploadMx.Lock()
	c.uploadsClosed = true
	var senders []*uploadSender
	if c.uploadIdleSender != nil {
		senders = append(senders, c.uploadIdleSender)
		c.uploadIdleSender = nil
	}
	for _, lease := range c.uploads {
		if lease.sender != nil {
			senders = append(senders, lease.sender)
		}
	}
	c.uploadMx.Unlock()
	c.uploadSenderMx.Unlock()
	for _, sender := range senders {
		sender.transport.Close()
	}
}

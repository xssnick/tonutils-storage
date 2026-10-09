package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type uploadTestPeer struct {
	testADNLPeer
	ctx     context.Context
	cancel  context.CancelFunc
	handler func(*adnl.MessageCustom) error
	onSend  func(context.Context, tl.Serializable) error
}

func newUploadTestPeer() *uploadTestPeer {
	ctx, cancel := context.WithCancel(context.Background())
	return &uploadTestPeer{ctx: ctx, cancel: cancel}
}

func (p *uploadTestPeer) SetCustomMessageHandler(handler func(*adnl.MessageCustom) error) {
	p.handler = handler
}
func (p *uploadTestPeer) GetCloserCtx() context.Context { return p.ctx }
func (p *uploadTestPeer) Close()                        { p.cancel() }

func (p *uploadTestPeer) SendCustomMessage(ctx context.Context, data tl.Serializable) error {
	if p.onSend != nil {
		return p.onSend(ctx, data)
	}
	return nil
}

func uploadBytes(c *PeerConnection) uint64 {
	c.uploadMx.Lock()
	defer c.uploadMx.Unlock()
	return c.uploadBytes
}

func TestUploadBudgetHeldUntilFinalRLDPCompletion(t *testing.T) {
	uploadVersions(t, func(t *testing.T, version rldp.Version) {
		conn, peer, _ := newUploadSenderFixture(t, version, nil)
		data := make([]byte, int(rldp.PartSize)*2+12)
		transfer := make([]byte, 32)
		lease, err := conn.reserveUpload(transfer, uint64(len(data)), time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		sender, err := conn.getUploadSender(lease)
		if err != nil {
			t.Fatal(err)
		}
		var completed atomic.Int32
		lease.setOnComplete(func() { completed.Add(1) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err = lease.sendAnswer(ctx, 1<<25, uint32(time.Now().Add(time.Minute).Unix()), make([]byte, 32), transfer, Piece{Data: data}); err != nil {
			t.Fatal(err)
		}
		if uploadBytes(conn) == 0 {
			t.Fatal("SendAnswer returned its permit before completion")
		}
		// An out-of-order final ACK is ignored by both transport and accounting.
		if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, transfer, 2)}); err != nil {
			t.Fatal(err)
		}
		if uploadBytes(conn) == 0 {
			t.Fatal("premature final ACK returned upload permit")
		}
		for part := uint32(0); part < 2; part++ {
			if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, transfer, part)}); err != nil {
				t.Fatal(err)
			}
			if uploadBytes(conn) == 0 {
				t.Fatalf("part %d completion returned upload permit", part)
			}
		}
		if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, transfer, 2)}); err != nil {
			t.Fatal(err)
		}
		if uploadBytes(conn) != 0 || completed.Load() != 1 || sender.Stats().Active.OutboundTransfers != 0 || sender.Stats().Version != version {
			t.Fatal("final completion did not release exactly once after transport retirement")
		}
		if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, transfer, 2)}); err != nil {
			t.Fatal(err)
		}
		if completed.Load() != 1 {
			t.Fatal("duplicate completion counted upload twice")
		}
	})
}

type uploadBudgetRLDP struct {
	testRLDP
	active atomic.Int32
	closed atomic.Bool
}

func (r *uploadBudgetRLDP) Stats() rldp.Stats {
	return rldp.Stats{Closed: r.closed.Load(), Active: rldp.ActiveStats{OutboundTransfers: int(r.active.Load())}}
}
func (r *uploadBudgetRLDP) Close() { r.closed.Store(true); r.active.Store(0) }

func TestUploadBudgetTimeoutWaitsForTransportRetirement(t *testing.T) {
	conn, _, _ := newUploadSenderFixture(t, rldp.Version2, nil)
	transfer := make([]byte, 32)
	lease, err := conn.reserveUpload(transfer, 4096, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err = lease.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+1), make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	conn.expireUploads(time.Now().Add(time.Hour))
	if uploadBytes(conn) == 0 {
		t.Fatal("expired permit released while its sender retained data")
	}
	waitUntil(t, 2*time.Second, func() bool { return uploadBytes(conn) == 0 }, "retired sender to return its permit")
}

func TestUploadBudgetConnectionCloseRetiresTransportBeforeRelease(t *testing.T) {
	peer := newUploadTestPeer()
	transport := &uploadBudgetRLDP{}
	transport.active.Store(1)
	conn := &PeerConnection{adnl: peer, rldp: transport, srv: &Server{}}
	lease, err := conn.reserveUpload(make([]byte, 32), 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var completed atomic.Int32
	lease.setOnComplete(func() { completed.Add(1) })
	lease.retainUntil(lease.deadline)
	peer.Close()
	deadline := time.Now().Add(time.Second)
	for uploadBytes(conn) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if uploadBytes(conn) != 0 || !transport.closed.Load() || completed.Load() != 0 {
		t.Fatal("connection close did not retire transport and release uncompleted upload")
	}
}

func TestUploadBudgetBoundsPeerAndServerBytes(t *testing.T) {
	server := &Server{}
	var peers []*PeerConnection
	for i := 0; i < 32; i++ {
		conn := &PeerConnection{srv: server}
		if _, err := conn.reserveUpload(make([]byte, 32), 8<<20, time.Now().Add(time.Minute)); err != nil {
			break
		}
		peers = append(peers, conn)
		if _, err := conn.reserveUpload([]byte{1}, 8<<20, time.Now().Add(time.Minute)); err == nil {
			t.Fatal("peer upload budget exceeded")
		}
	}
	if len(peers) != 7 {
		t.Fatalf("global budget admitted %d large transfers, want 7", len(peers))
	}
	for _, peer := range peers {
		peer.releaseUploads()
	}
	server.uploadBudget.mx.Lock()
	used := server.uploadBudget.used
	server.uploadBudget.mx.Unlock()
	if used != 0 {
		t.Fatalf("global upload memory leaked %d bytes", used)
	}
}

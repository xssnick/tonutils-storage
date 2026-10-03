package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

func newUploadSenderFixture(t *testing.T, version rldp.Version, onSend func(context.Context, tl.Serializable) error) (*PeerConnection, *uploadTestPeer, *rldp.RLDP) {
	t.Helper()
	peer := newUploadTestPeer()
	peer.onSend = onSend
	conn := &PeerConnection{adnl: peer, srv: &Server{}}
	ext := overlay.CreateExtendedADNL(&uploadTrackingPeer{Peer: peer, conn: conn})
	var primary *rldp.RLDP
	if version == rldp.Version1 {
		primary = rldp.NewClient(ext)
	} else {
		primary = rldp.NewClientV2(ext)
	}
	conn.rldp = primary
	t.Cleanup(func() { primary.Close(); conn.closeUploadSender(); peer.Close(); conn.releaseUploads() })
	return conn, peer, primary
}

func TestUploadBudgetTimeoutReclaimsWhilePrimaryQueriesRemainActive(t *testing.T) {
	conn, _, primary := newUploadSenderFixture(t, rldp.Version2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := primary.DoQueryAsync(ctx, 1<<20, make([]byte, 32), GetPiece{PieceID: 0}, make(chan rldp.AsyncQueryResult, 1)); err != nil {
		t.Fatal(err)
	}
	sender, err := conn.getUploadSender()
	if err != nil {
		t.Fatal(err)
	}
	transfer := make([]byte, 32)
	lease, err := conn.reserveUpload(transfer, 4096, time.Now().Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = sender.SendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+1), make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	lease.retainUntil(lease.deadline)
	waitUntil(t, 2*time.Second, func() bool { return sender.Stats().Active.OutboundTransfers == 0 }, "bulk sender to expire its answer")
	if primary.Stats().Active.OutboundTransfers == 0 {
		t.Fatal("expected unrelated primary query to remain active")
	}
	conn.expireUploads(time.Now().Add(10 * time.Second))
	if uploadBytes(conn) != 0 {
		t.Fatal("unrelated primary query blocked expired upload permit reclamation")
	}
}

func TestUploadSenderDoesNotDecodePrimaryAnswerPayloads(t *testing.T) {
	firstPart := make(chan rldp.MessagePart, 1)
	conn, peer, primary := newUploadSenderFixture(t, rldp.Version2, func(_ context.Context, data tl.Serializable) error {
		if part, ok := data.(rldp.MessagePartV2); ok {
			select {
			case firstPart <- rldp.MessagePart(part):
			default:
			}
		}
		return nil
	})
	sender, err := conn.getUploadSender()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	queryID := make([]byte, 32)
	if err = primary.DoQueryAsync(ctx, 1<<20, queryID, GetPiece{PieceID: 0}, make(chan rldp.AsyncQueryResult, 1)); err != nil {
		t.Fatal(err)
	}
	queryPart := <-firstPart
	remotePeer := newUploadTestPeer()
	remotePeer.onSend = func(_ context.Context, data tl.Serializable) error {
		return peer.handler(&adnl.MessageCustom{Data: data})
	}
	remote := rldp.NewClientV2(remotePeer)
	t.Cleanup(func() { remote.Close(); remotePeer.Close() })
	if err = remote.SendAnswer(ctx, 1<<20, uint32(time.Now().Add(time.Minute).Unix()), queryID, queryPart.TransferID, Piece{Data: make([]byte, 128<<10)}); err != nil {
		t.Fatal(err)
	}
	if primary.Stats().Active.InboundStreams == 0 {
		t.Fatal("primary transport did not receive its large answer")
	}
	if sender.Stats().Active.InboundStreams != 0 || sender.Stats().Inbound.TransfersStarted != 0 {
		t.Fatal("bulk sender duplicated primary answer decoding")
	}
}

func TestUploadSenderPreservesLegacyVersion(t *testing.T) {
	var sentLegacy atomic.Bool
	conn, peer, _ := newUploadSenderFixture(t, rldp.Version1, func(_ context.Context, data tl.Serializable) error {
		if _, ok := data.(rldp.MessagePart); ok {
			sentLegacy.Store(true)
		}
		return nil
	})
	sender, err := conn.getUploadSender()
	if err != nil {
		t.Fatal(err)
	}
	transfer := make([]byte, 32)
	lease, err := conn.reserveUpload(transfer, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err = sender.SendAnswer(ctx, 1<<20, uint32(time.Now().Add(time.Minute).Unix()), make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	lease.retainUntil(lease.deadline)
	if err = peer.handler(&adnl.MessageCustom{Data: rldp.Complete{TransferID: []byte(reverseUploadTransferID(transfer)), Part: 0}}); err != nil {
		t.Fatal(err)
	}
	if !sentLegacy.Load() || sender.Stats().Version != rldp.Version1 || uploadBytes(conn) != 0 {
		t.Fatal("legacy upload protocol or final completion changed")
	}
}

func TestUploadSenderConnectionCloseRetiresBothTransports(t *testing.T) {
	conn, peer, primary := newUploadSenderFixture(t, rldp.Version2, nil)
	sender, err := conn.getUploadSender()
	if err != nil {
		t.Fatal(err)
	}
	transfer := make([]byte, 32)
	lease, err := conn.reserveUpload(transfer, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err = sender.SendAnswer(ctx, 1<<20, uint32(time.Now().Add(time.Minute).Unix()), make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	lease.retainUntil(lease.deadline)
	peer.Close()
	waitUntil(t, time.Second, func() bool { return uploadBytes(conn) == 0 }, "connection close to return bulk memory")
	if !primary.Stats().Closed || !sender.Stats().Closed {
		t.Fatal("connection close left a transport active after releasing its memory")
	}
	if _, err = conn.getUploadSender(); err == nil {
		t.Fatal("closed connection created a new bulk sender")
	}
}

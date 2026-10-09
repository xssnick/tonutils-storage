package storage

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

func uploadVersions(t *testing.T, run func(*testing.T, rldp.Version)) {
	t.Helper()
	for _, test := range []struct {
		name    string
		version rldp.Version
	}{{"v1", rldp.Version1}, {"v2", rldp.Version2}} {
		t.Run(test.name, func(t *testing.T) { run(t, test.version) })
	}
}

func uploadCompletion(version rldp.Version, transfer []byte, part uint32) tl.Serializable {
	complete := rldp.Complete{TransferID: []byte(reverseUploadTransferID(transfer)), Part: part}
	if version == rldp.Version2 {
		return rldp.CompleteV2(complete)
	}
	return complete
}

func TestUploadRetiresIndependentlyOfOverlappingAnswers(t *testing.T) {
	uploadVersions(t, func(t *testing.T, version rldp.Version) {
		synctest.Test(t, func(t *testing.T) {
			conn, peer, primary := newUploadSenderFixture(t, version, nil)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			expiredID, busyID := make([]byte, 32), make([]byte, 32)
			busyID[0] = 1
			expired, err := conn.reserveUpload(expiredID, 4096, time.Now().Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			busy, err := conn.reserveUpload(busyID, 4096, time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			expiredSender, err := conn.getUploadSender(expired)
			if err != nil {
				t.Fatal(err)
			}
			busySender, err := conn.getUploadSender(busy)
			if err != nil {
				t.Fatal(err)
			}
			if expiredSender == busySender {
				t.Fatal("overlapping answers share a sender")
			}
			var expiredCompleted, busyCompleted atomic.Int32
			expired.setOnComplete(func() { expiredCompleted.Add(1) })
			busy.setOnComplete(func() { busyCompleted.Add(1) })
			answer := Piece{Data: make([]byte, 4096)}
			if err = expired.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+1), make([]byte, 32), expiredID, answer); err != nil {
				t.Fatal(err)
			}
			if err = busy.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), busyID, answer); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Second)
			synctest.Wait()
			if expiredSender.Stats().Active.OutboundTransfers != 0 || busySender.Stats().Active.OutboundTransfers != 1 {
				t.Fatal("unexpected transport retirement state")
			}
			if got := uploadBytes(conn); got != busy.size {
				t.Fatalf("expired answer retained permit: got %d, want %d", got, busy.size)
			}
			if expiredCompleted.Load() != 0 {
				t.Fatal("timeout counted completed upload")
			}
			if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, expiredID, 0)}); err != nil {
				t.Fatal(err)
			}
			if busySender.Stats().Active.OutboundTransfers != 1 || expiredCompleted.Load() != 0 {
				t.Fatal("late ACK affected unrelated answer")
			}
			if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, busyID, 0)}); err != nil {
				t.Fatal(err)
			}
			if uploadBytes(conn) != 0 || busyCompleted.Load() != 1 {
				t.Fatal("busy answer completion did not return its own permit exactly once")
			}
			if primary.Stats().Closed || peer.GetCloserCtx().Err() != nil {
				t.Fatal("retirement closed shared ADNL or primary transport")
			}
			conn.srv.uploadBudget.mx.Lock()
			global := conn.srv.uploadBudget.used
			conn.srv.uploadBudget.mx.Unlock()
			if global != 0 {
				t.Fatalf("global budget leaked %d bytes", global)
			}
		})
	})
}

func TestUploadFinalACKWaitsForInitialSendBurst(t *testing.T) {
	uploadVersions(t, func(t *testing.T, version rldp.Version) {
		var conn *PeerConnection
		var peer *uploadTestPeer
		var completed atomic.Int32
		var acknowledged atomic.Bool
		var err error
		conn, peer, _ = newUploadSenderFixture(t, version, func(_ context.Context, data tl.Serializable) error {
			var part rldp.MessagePart
			switch message := data.(type) {
			case rldp.MessagePart:
				part = message
			case rldp.MessagePartV2:
				part = rldp.MessagePart(message)
			default:
				return nil
			}
			if acknowledged.CompareAndSwap(false, true) {
				complete := rldp.Complete{TransferID: part.TransferID, Part: part.Part}
				var message tl.Serializable = complete
				if version == rldp.Version2 {
					message = rldp.CompleteV2(complete)
				}
				if err := peer.handler(&adnl.MessageCustom{Data: message}); err != nil {
					return err
				}
			}
			if uploadBytes(conn) == 0 || completed.Load() != 0 {
				t.Error("ACK returned permit/callback while SendAnswer still owned the initial burst")
			}
			return nil
		})
		transfer := make([]byte, 32)
		lease, err := conn.reserveUpload(transfer, 4096, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		lease.setOnComplete(func() { completed.Add(1) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err = lease.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)}); err != nil {
			t.Fatal(err)
		}
		if !acknowledged.Load() || uploadBytes(conn) != 0 || completed.Load() != 1 {
			t.Fatal("initial burst did not finish pending completion exactly once")
		}
	})
}

func TestUploadRetirementWaitsForBlockedRecoverySend(t *testing.T) {
	uploadVersions(t, func(t *testing.T, version rldp.Version) {
		for _, reason := range []string{"ack", "timeout", "sender close", "disconnect"} {
			t.Run(reason, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var lease *uploadLease
					var conn *PeerConnection
					blocked, unblock := make(chan struct{}), make(chan struct{})
					var stopped atomic.Bool
					var completed atomic.Int32
					conn, peer, primary := newUploadSenderFixture(t, version, func(_ context.Context, _ tl.Serializable) error {
						conn.uploadMx.Lock()
						initial := lease.sending
						conn.uploadMx.Unlock()
						if !initial && stopped.CompareAndSwap(false, true) {
							close(blocked)
							<-unblock
						}
						return nil
					})
					transfer := make([]byte, 32)
					var err error
					lease, err = conn.reserveUpload(transfer, 4096, time.Now().Add(time.Minute))
					if err != nil {
						t.Fatal(err)
					}
					lease.setOnComplete(func() { completed.Add(1) })
					sender, err := conn.getUploadSender(lease)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					timeout := uint32(time.Now().Unix() + 60)
					if reason == "timeout" {
						timeout = uint32(time.Now().Unix() + 1)
					}
					if err = lease.sendAnswer(ctx, 1<<20, timeout, make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)}); err != nil {
						t.Fatal(err)
					}
					<-blocked
					switch reason {
					case "ack":
						err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(version, transfer, 0)})
					case "timeout":
						time.Sleep(2 * time.Second)
						conn.expireUploads(time.Now())
					case "sender close":
						sender.Close()
						conn.expireUploads(time.Now())
					case "disconnect":
						peer.Close()
						synctest.Wait()
					}
					if err != nil {
						t.Fatal(err)
					}
					if uploadBytes(conn) == 0 || completed.Load() != 0 {
						t.Fatal("blocked recovery send lost its permit or delivered completion")
					}
					if reason != "disconnect" {
						nextID := make([]byte, 32)
						nextID[0] = 1
						next, err := conn.reserveUpload(nextID, 4096, time.Now().Add(time.Minute))
						if err != nil {
							t.Fatal(err)
						}
						nextSender, err := conn.getUploadSender(next)
						if err != nil {
							t.Fatal(err)
						}
						if nextSender == sender {
							t.Fatal("sender reused while recovery send still owns data")
						}
						next.release(false)
						if primary.Stats().Closed || peer.GetCloserCtx().Err() != nil {
							t.Fatal("isolated retirement closed shared transport")
						}
					} else if _, err := conn.reserveUpload([]byte{1}, 4096, time.Now()); err == nil {
						t.Fatal("disconnect admitted a new upload")
					}
					close(unblock)
					time.Sleep(200 * time.Millisecond)
					synctest.Wait()
					if uploadBytes(conn) != 0 {
						t.Fatal("drained recovery send retained its permit")
					}
					wantCompleted := int32(0)
					if reason == "ack" {
						wantCompleted = 1
					}
					if completed.Load() != wantCompleted {
						t.Fatalf("completion count=%d,want=%d", completed.Load(), wantCompleted)
					}
				})
			})
		}
	})
}

func TestUploadSendErrorReturnsPermitWithoutClosingSharedTransport(t *testing.T) {
	uploadVersions(t, func(t *testing.T, version rldp.Version) {
		forced := errors.New("forced send failure")
		conn, peer, primary := newUploadSenderFixture(t, version, func(context.Context, tl.Serializable) error { return forced })
		transfer := make([]byte, 32)
		lease, err := conn.reserveUpload(transfer, 4096, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		var completed atomic.Int32
		lease.setOnComplete(func() { completed.Add(1) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		err = lease.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), transfer, Piece{Data: make([]byte, 4096)})
		if !errors.Is(err, forced) {
			t.Fatalf("send error=%v,want forced failure", err)
		}
		if uploadBytes(conn) != 0 || completed.Load() != 0 {
			t.Fatal("send failure retained permit or counted successful upload")
		}
		if primary.Stats().Closed || peer.GetCloserCtx().Err() != nil {
			t.Fatal("failed answer closed unrelated transport")
		}
	})
}

func TestUploadDelayedACKDoesNotChangeReusedSender(t *testing.T) {
	conn, peer, _ := newUploadSenderFixture(t, rldp.Version2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	oldID, newID := make([]byte, 32), make([]byte, 32)
	newID[0] = 1
	old, err := conn.reserveUpload(oldID, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	oldSender, err := conn.getUploadSender(old)
	if err != nil {
		t.Fatal(err)
	}
	answer := Piece{Data: make([]byte, 4096)}
	if err = old.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), oldID, answer); err != nil {
		t.Fatal(err)
	}
	old.ackMx.Lock()
	done := make(chan error, 1)
	go func() { done <- peer.handler(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version1, oldID, 0)}) }()
	// Wait until the real handler has captured the old lease and is blocked on
	// ACK serialization. A timer delay alone would not establish this ordering.
	deadline := time.Now().Add(time.Second)
	for {
		stack := make([]byte, 1<<18)
		stack = stack[:runtime.Stack(stack, true)]
		blocked := strings.Contains(string(stack), "sync.(*Mutex).lockSlow") && strings.Contains(string(stack), "(*uploadTrackingPeer).SetCustomMessageHandler.func1")
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			old.ackMx.Unlock()
			t.Fatal("old ACK handler did not reach serialization lock")
		}
		runtime.Gosched()
	}
	// Retire through the real decoder while the delayed handler is waiting,
	// then return the idle sender for a different transfer ID.
	if callback := old.sender.adnl.handler.Load(); callback != nil {
		if err = (*callback)(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version2, oldID, 0)}); err != nil {
			old.ackMx.Unlock()
			t.Fatal(err)
		}
	}
	old.release(false)
	next, err := conn.reserveUpload(newID, 4096, time.Now().Add(time.Minute))
	if err != nil {
		old.ackMx.Unlock()
		t.Fatal(err)
	}
	nextSender, err := conn.getUploadSender(next)
	if err != nil {
		old.ackMx.Unlock()
		t.Fatal(err)
	}
	if nextSender != oldSender {
		old.ackMx.Unlock()
		t.Fatal("idle sender was not reused")
	}
	if err = next.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), newID, answer); err != nil {
		old.ackMx.Unlock()
		t.Fatal(err)
	}
	old.ackMx.Unlock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if nextSender.Stats().Version != rldp.Version2 || nextSender.Stats().Active.OutboundTransfers != 1 || uploadBytes(conn) != next.size {
		t.Fatal("delayed old ACK reached the reused sender")
	}
	if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version2, newID, 0)}); err != nil {
		t.Fatal(err)
	}
	if uploadBytes(conn) != 0 {
		t.Fatal("new owner did not complete")
	}
}

func TestUploadValidatedControlHandlerPinsSenderUntilCallbackReturns(t *testing.T) {
	conn, peer, _ := newUploadSenderFixture(t, rldp.Version2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	oldID, newID := make([]byte, 32), make([]byte, 32)
	newID[0] = 1
	old, err := conn.reserveUpload(oldID, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	oldSender, err := conn.getUploadSender(old)
	if err != nil {
		t.Fatal(err)
	}
	answer := Piece{Data: make([]byte, 4096)}
	if err = old.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), oldID, answer); err != nil {
		t.Fatal(err)
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	var paused atomic.Bool
	tracked := &uploadTrackingPeer{Peer: peer, conn: conn}
	tracked.SetCustomMessageHandler(func(message *adnl.MessageCustom) error {
		id, _ := uploadControlTransfer(message.Data)
		if id == old.key && paused.CompareAndSwap(false, true) {
			close(entered)
			<-proceed
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- peer.handler(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version1, oldID, 0)}) }()
	<-entered
	// The real answer decoder retires the old transfer while the primary
	// callback is blocked, after ownership validation has already succeeded.
	if callback := old.sender.adnl.handler.Load(); callback != nil {
		if err = (*callback)(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version2, oldID, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	old.release(false)
	if uploadBytes(conn) != old.size {
		t.Fatal("validated handler lost its permit before callback returned")
	}
	next, err := conn.reserveUpload(newID, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	nextSender, err := conn.getUploadSender(next)
	if err != nil {
		t.Fatal(err)
	}
	if nextSender == oldSender {
		t.Fatal("sender pooled while validated control callback was still running")
	}
	if err = next.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), newID, answer); err != nil {
		t.Fatal(err)
	}
	close(proceed)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if nextSender.Stats().Version != rldp.Version2 || nextSender.Stats().Active.OutboundTransfers != 1 || uploadBytes(conn) != next.size {
		t.Fatal("old callback reached a different transfer owner")
	}
	if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version2, newID, 0)}); err != nil {
		t.Fatal(err)
	}
	if uploadBytes(conn) != 0 {
		t.Fatal("new transfer did not complete")
	}
}

func TestUploadIdleSenderMatchesNegotiatedProtocol(t *testing.T) {
	conn, peer, primary := newUploadSenderFixture(t, rldp.Version2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	firstID, secondID := make([]byte, 32), make([]byte, 32)
	secondID[0] = 1
	first, err := conn.reserveUpload(firstID, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	firstSender, err := conn.getUploadSender(first)
	if err != nil {
		t.Fatal(err)
	}
	answer := Piece{Data: make([]byte, 4096)}
	if err = first.sendAnswer(ctx, 1<<20, uint32(time.Now().Unix()+60), make([]byte, 32), firstID, answer); err != nil {
		t.Fatal(err)
	}
	if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version2, firstID, 0)}); err != nil {
		t.Fatal(err)
	}
	// An ordinary V1 control frame negotiates the primary transport back to V1.
	if err = peer.handler(&adnl.MessageCustom{Data: uploadCompletion(rldp.Version1, make([]byte, 32), 0)}); err != nil {
		t.Fatal(err)
	}
	if primary.Stats().Version != rldp.Version1 {
		t.Fatal("primary did not negotiate V1")
	}
	second, err := conn.reserveUpload(secondID, 4096, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	secondSender, err := conn.getUploadSender(second)
	if err != nil {
		t.Fatal(err)
	}
	if secondSender == firstSender || secondSender.Stats().Version != rldp.Version1 || !firstSender.Stats().Closed {
		t.Fatal("idle sender reused an obsolete protocol version")
	}
	second.release(false)
	if primary.Stats().Closed || peer.GetCloserCtx().Err() != nil {
		t.Fatal("discarding old sender closed shared transport")
	}
}

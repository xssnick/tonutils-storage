package storage

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type regressionAnswerRLDP struct {
	testRLDP
	peer   rldp.ADNL
	answer tl.Serializable
}

func (r *regressionAnswerRLDP) GetADNL() rldp.ADNL { return r.peer }

func (r *regressionAnswerRLDP) SendAnswer(_ context.Context, _ uint64, _ uint32, _, _ []byte, answer tl.Serializable) error {
	r.answer = answer
	return nil
}

func TestServerGetPieceBeforeMetadataReturnsStorageError(t *testing.T) {
	for _, test := range []struct {
		name       string
		withUpload bool
		header     *TorrentHeader
		info       *TorrentInfo
		message    string
	}{
		{name: "download only", message: "bag is not for upload"},
		{name: "upload enabled before header", withUpload: true, message: "bag is not for upload"},
		{name: "header without info", withUpload: true, header: &TorrentHeader{}, message: "torrent metadata is not loaded"},
		{name: "zero piece size", withUpload: true, header: &TorrentHeader{}, info: &TorrentInfo{}, message: "torrent metadata is not loaded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			torrent := NewTorrent(t.TempDir(), nil, nil)
			torrent.globalCtx = t.Context()
			torrent.activeUpload = test.withUpload
			torrent.Header = test.header
			torrent.Info = test.info
			overlayID := []byte("overlay")
			store := newE2EStorage(4096)
			store.torrentsByOver[string(overlayID)] = torrent
			server := &Server{store: store}
			transport := &regressionAnswerRLDP{peer: &testADNLPeer{}}
			peer := &overlay.RLDPWrapper{RLDP: transport}
			query := &rldp.Query{Data: overlay.WrapQuery(overlayID, GetPiece{PieceID: 0})}

			if err := server.handleRLDPQuery(peer)(make([]byte, 32), query); err != nil {
				t.Fatal(err)
			}
			answer, ok := transport.answer.(StorageError)
			if !ok || answer.Message != test.message {
				t.Fatalf("unexpected response: %#v", transport.answer)
			}
		})
	}
}

type regressionCanceledADNLPeer struct {
	testADNLPeer
}

func (p *regressionCanceledADNLPeer) Query(ctx context.Context, _, _ tl.Serializable) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestConnectToNodeCallerTimeoutCancelsInitialization(t *testing.T) {
	for _, admissionFull := range []bool{true, false} {
		name := "session admission"
		if !admissionFull {
			name = "session ping"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pub, _, err := ed25519.GenerateKey(nil)
				if err != nil {
					t.Fatal(err)
				}
				node := &overlay.Node{ID: keys.PublicKeyED25519{Key: pub}, Overlay: []byte("overlay")}
				id, err := tl.Hash(node.ID)
				if err != nil {
					t.Fatal(err)
				}
				server := &Server{
					closeCtx:         t.Context(),
					sessionSemaphore: make(chan struct{}, 1),
					bootstrapped:     map[string]*PeerConnection{},
				}
				if admissionFull {
					server.sessionSemaphore <- struct{}{}
				}
				conn := &PeerConnection{
					adnl:             &regressionCanceledADNLPeer{},
					srv:              server,
					usedByBags:       map[string]*storagePeer{},
					bagsInitQueue:    make(chan struct{}, 8),
					initControlQueue: make(chan struct{}, 2),
				}
				server.bootstrapped[hex.EncodeToString(id)] = conn
				bagCtx, stopBag := context.WithCancel(t.Context())
				defer stopBag()
				torrent := &Torrent{
					BagID:     []byte("bag"),
					Info:      &TorrentInfo{PieceSize: 4096, FileSize: 4096},
					globalCtx: bagCtx,
					peers:     map[string]*PeerInfo{},
					wake:      newWakeSig(),
				}
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- server.ConnectToNode(ctx, torrent, node, nil) }()
				synctest.Wait()
				if len(conn.bagsInitQueue) != 1 {
					t.Fatal("connection did not reach initialization")
				}
				time.Sleep(20 * time.Millisecond)
				synctest.Wait()
				select {
				case err := <-done:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("expected caller timeout, got %v", err)
					}
				default:
					t.Fatal("connection initialization ignored the caller timeout")
				}
				if len(conn.bagsInitQueue) != 0 || conn.GetFor(torrent.BagID) != nil {
					t.Fatal("canceled initialization retained a bag or queue slot")
				}
			})
		})
	}
}

func TestPeerManagerRestartPreservesReplacementPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		torrent := NewTorrent(t.TempDir(), newE2EStorage(4096), nil)
		torrent.BagID = []byte("bag")
		torrent.Info = &TorrentInfo{PieceSize: 4096, FileSize: 4096, HeaderSize: 1}
		torrent.CreatedLocally = true
		if err := torrent.Start(true, true, false); err != nil {
			t.Fatal(err)
		}
		defer func() { torrent.Stop(); torrent.Wait() }()

		// Hold the peer map as an incoming update can, so the old manager's
		// shutdown waits until a peer for the replacement lifetime is published.
		torrent.peersMx.Lock()
		if err := torrent.Start(true, true, true); err != nil {
			torrent.peersMx.Unlock()
			t.Fatal(err)
		}
		conn := &PeerConnection{adnl: &testADNLPeer{}, usedByBags: map[string]*storagePeer{}}
		sessionID := int64(1)
		peer, _ := torrent.prepareStoragePeer([]byte("overlay"), nil, conn, &sessionID)
		torrent.touchPeer(peer)
		torrent.peersMx.Unlock()
		synctest.Wait()
		if peer.closerCtx.Err() != nil || torrent.GetPeer(peer.nodeId) == nil {
			t.Fatal("old manager closed a peer belonging to the replacement lifetime")
		}

		torrent.Stop()
		torrent.Wait()
		if peer.closerCtx.Err() == nil || torrent.GetPeer(peer.nodeId) != nil || conn.GetFor(torrent.BagID) != nil {
			t.Fatal("stopping the current manager did not close its peer")
		}
	})
}

func TestStoragePeerClosePreservesReplacementPeerRecord(t *testing.T) {
	torrent := &Torrent{BagID: []byte("bag"), peers: map[string]*PeerInfo{}, wake: newWakeSig()}
	transport := newUploadTestPeer()
	defer transport.Close()
	conn := &PeerConnection{adnl: transport, usedByBags: map[string]*storagePeer{}}
	oldPeer := &storagePeer{torrent: torrent, nodeId: []byte("node"), conn: conn, stop: func() {}}
	replacement := &storagePeer{torrent: torrent, nodeId: oldPeer.nodeId, conn: conn}
	conn.UseFor(replacement)
	torrent.TouchPeer(replacement)

	oldPeer.Close()
	if info := torrent.GetPeer(replacement.nodeId); info == nil || info.peer != replacement {
		t.Fatal("old peer closure deleted the replacement peer record")
	}
	if conn.GetFor(torrent.BagID) != replacement {
		t.Fatal("old peer closure released the replacement connection usage")
	}
	if transport.GetCloserCtx().Err() != nil {
		t.Fatal("old peer closure closed the replacement transport")
	}
}

func newRegressionConnection(t *testing.T) (*Server, *Torrent, *overlay.Node, *PeerConnection) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	node := &overlay.Node{ID: keys.PublicKeyED25519{Key: pub}, Overlay: []byte("overlay")}
	id, err := tl.Hash(node.ID)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{
		closeCtx:         t.Context(),
		sessionSemaphore: make(chan struct{}, 1),
		bootstrapped:     map[string]*PeerConnection{},
	}
	transport := newUploadTestPeer()
	t.Cleanup(transport.Close)
	conn := &PeerConnection{
		adnl:             transport,
		rldp:             &testRLDP{},
		srv:              server,
		usedByBags:       map[string]*storagePeer{},
		bagsInitQueue:    make(chan struct{}, 1),
		initControlQueue: make(chan struct{}, 2),
	}
	server.bootstrapped[hex.EncodeToString(id)] = conn
	torrent := NewTorrent(t.TempDir(), newE2EStorage(4096), nil)
	torrent.BagID = []byte("bag")
	torrent.Info = &TorrentInfo{PieceSize: 4096, FileSize: 4096, HeaderSize: 1}
	torrent.pieceMask = []byte{1}
	torrent.globalCtx, torrent.pause = context.WithCancel(t.Context())
	t.Cleanup(torrent.pause)
	return server, torrent, node, conn
}

func TestConnectToNodeTorrentStopCancelsAdmissionAndReadiness(t *testing.T) {
	for _, admissionFull := range []bool{true, false} {
		name := "bag admission"
		if !admissionFull {
			name = "remote readiness"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				server, torrent, node, conn := newRegressionConnection(t)
				if admissionFull {
					conn.bagsInitQueue <- struct{}{}
				}
				done := make(chan error, 1)
				go func() { done <- server.ConnectToNode(t.Context(), torrent, node, nil) }()
				synctest.Wait()
				peer := conn.GetFor(torrent.BagID)
				if peer == nil || (!admissionFull && !peer.isLocalInitSent()) {
					t.Fatal("connection did not reach the expected wait stage")
				}
				torrent.Stop()
				synctest.Wait()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected bag cancellation, got %v", err)
					}
				default:
					t.Fatal("connection ignored the stopped torrent")
				}
				if conn.GetFor(torrent.BagID) != nil {
					t.Fatal("canceled owned session retained its connection usage")
				}
				if !admissionFull && len(conn.bagsInitQueue) != 0 {
					t.Fatal("canceled readiness retained its queue slot")
				}
			})
		})
	}
}

func TestConnectToNodeCanceledReusePreservesHealthyPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, torrent, node, conn := newRegressionConnection(t)
		peer, _ := torrent.prepareStoragePeer(node.Overlay, node, conn, nil)
		atomic.StoreInt32(&peer.session.localInitSent, 1)
		peer.markRemoteInitComplete()
		peer.touch()
		conn.bagsInitQueue <- struct{}{}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- server.ConnectToNode(ctx, torrent, node, nil) }()
		synctest.Wait()
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected caller timeout, got %v", err)
			}
		default:
			t.Fatal("reused connection ignored the caller timeout")
		}
		if peer.closerCtx.Err() != nil || conn.GetFor(torrent.BagID) != peer || torrent.GetPeer(peer.nodeId) == nil {
			t.Fatal("canceling a duplicate caller closed the healthy peer")
		}
		if conn.adnl.GetCloserCtx().Err() != nil {
			t.Fatal("canceling a duplicate caller closed its shared transport")
		}
	})
}

func TestConnectToNodeStaleCancellationPreservesReinitializedSession(t *testing.T) {
	for _, admissionFull := range []bool{true, false} {
		name := "bag admission"
		if !admissionFull {
			name = "remote readiness"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				server, torrent, node, conn := newRegressionConnection(t)
				if admissionFull {
					conn.bagsInitQueue <- struct{}{}
				}
				done := make(chan error, 1)
				go func() { done <- server.ConnectToNode(t.Context(), torrent, node, nil) }()
				synctest.Wait()
				peer := conn.GetFor(torrent.BagID)
				if peer == nil {
					t.Fatal("connection did not prepare its session")
				}
				newID := atomic.LoadInt64(&peer.session.sessionId) + 1
				replacement, next := torrent.prepareStoragePeer(node.Overlay, node, conn, &newID)
				atomic.StoreInt32(&replacement.session.localInitSent, 1)
				replacement.markRemoteInitComplete()
				replacement.touch()
				synctest.Wait()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) && !errors.Is(err, errStaleSessionAttempt) {
						t.Fatalf("expected superseded connection cancellation, got %v", err)
					}
				default:
					t.Fatal("superseded session retained its connection waiter")
				}
				if replacement != peer || next == nil || next.ctx.Err() != nil || replacement.closerCtx.Err() != nil ||
					!replacement.isSessionAttemptCurrent(*next) || conn.GetFor(torrent.BagID) != replacement || torrent.GetPeer(replacement.nodeId) == nil {
					t.Fatal("stale connection cancellation closed the replacement generation")
				}
				if conn.adnl.GetCloserCtx().Err() != nil {
					t.Fatal("stale connection cancellation closed the replacement transport")
				}
			})
		})
	}
}

func TestConnectToNodeSuccessPreservesPeerLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, torrent, node, conn := newRegressionConnection(t)
		conn.rldp = &testRLDP{onQuery: func(query tl.Serializable, _ tl.Serializable) error {
			req, _ := overlay.UnwrapQuery(query)
			if _, ok := req.(AddUpdate); ok {
				conn.GetFor(torrent.BagID).markRemoteInitComplete()
			}
			return nil
		}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := server.ConnectToNode(ctx, torrent, node, nil); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		peer := conn.GetFor(torrent.BagID)
		if peer == nil || peer.closerCtx.Err() != nil || !peer.isSessionReady() || torrent.GetPeer(peer.nodeId) == nil {
			t.Fatal("successful connection retained the caller's temporary lifetime")
		}
		if len(conn.bagsInitQueue) != 0 || len(server.sessionSemaphore) != 0 {
			t.Fatal("successful connection retained initialization permits")
		}
	})
}

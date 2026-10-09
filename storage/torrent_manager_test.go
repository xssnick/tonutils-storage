package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/tl"
)

func TestPeerManagerRetriesFinalHavePiecesAfterQueueBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queried := make(chan struct{}, 1)
		transport := &testRLDP{onQuery: func(tl.Serializable, tl.Serializable) error {
			select {
			case queried <- struct{}{}:
			default:
			}
			return nil
		}}
		conn := &PeerConnection{adnl: &testADNLPeer{}, rldp: transport, controlQueue: make(chan struct{}, 1), usedByBags: map[string]*storagePeer{}}
		conn.controlQueue <- struct{}{}
		ctx, cancel := context.WithCancel(t.Context())
		torrent := &Torrent{
			BagID: []byte("bag"), Info: &TorrentInfo{PieceSize: 1, FileSize: 8},
			peers: map[string]*PeerInfo{}, knownNodes: map[string]*KnownNode{},
			signalNewPieces: make(chan struct{}, 1), newPieces: []uint32{7}, wake: newWakeSig(), globalCtx: ctx,
		}
		peer := &storagePeer{
			torrent: torrent, nodeId: []byte("peer"), conn: conn, stop: func() {},
			initSnapshotStarted: true, lastPingAt: time.Now(), lastNeighboursAt: time.Now(),
			session: peerSessionState{sessionId: 1, sessionGen: 1, localInitSent: 1, remoteInitComplete: 1},
		}
		conn.UseFor(peer)
		peer.touch()
		torrent.signalNewPieces <- struct{}{}
		torrent.opWg.Add(1)
		go torrent.peersManager(ctx)
		defer func() { cancel(); torrent.Wait() }()
		synctest.Wait()
		select {
		case <-queried:
			t.Fatal("announcement bypassed the busy control queue")
		default:
		}
		if atomic.LoadUint64(&peer.lastSentNewPiecesPos) != 0 {
			t.Fatal("busy queue advanced the announcement cursor")
		}
		<-conn.controlQueue
		select {
		case <-queried:
		case <-time.After(3 * time.Second):
			t.Fatal("last piece announcement was not retried")
		}
		cancel()
		torrent.Wait()
		if atomic.LoadUint64(&peer.lastSentNewPiecesPos) != 1 {
			t.Fatal("successful retry did not publish its piece cursor")
		}
	})
}

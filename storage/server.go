package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	key            ed25519.PrivateKey
	dht            *dht.Client
	gate           *adnl.Gateway
	store          Storage
	closeCtx       context.Context
	serverMode     bool
	dhtParallelism int

	bootstrapped map[string]*PeerConnection
	mx           sync.RWMutex

	registeredAddrs map[string]*address.List
	addrMx          sync.RWMutex

	closer func()
}

func NewServer(dht *dht.Client, gate *adnl.Gateway, key ed25519.PrivateKey, serverMode bool, dhtParallelism int) *Server {
	s := &Server{
		key:             key,
		dht:             dht,
		gate:            gate,
		bootstrapped:    map[string]*PeerConnection{},
		serverMode:      serverMode,
		dhtParallelism:  dhtParallelism,
		registeredAddrs: map[string]*address.List{},
	}
	s.closeCtx, s.closer = context.WithCancel(context.Background())
	s.gate.SetConnectionHandler(s.bootstrapPeerWrap)

	go s.startPeerSearcher()

	if serverMode {
		go func() {
			wait := 1 * time.Second
			// refresh dht records
			for {
				select {
				case <-s.closeCtx.Done():
					Logger("[STORAGE_DHT] STOPPED DHT UPDATER")
					return
				case <-time.After(wait):
				}

				Logger("[STORAGE_DHT] UPDATING OUR ADDRESS RECORD...")

				ctx, cancel := context.WithTimeout(s.closeCtx, 180*time.Second)
				err := s.updateDHT(ctx)
				cancel()

				if err != nil {
					Logger("[STORAGE_DHT] FAILED TO UPDATE OUR ADDRESS RECORD", err.Error())

					// on err, retry sooner
					wait = 5 * time.Second
					continue
				}
				wait = 1 * time.Minute

				Logger("[STORAGE_DHT] OUR ADDRESS RECORD UPDATED")
			}
		}()
	}

	return s
}

func (s *Server) SetStorage(store Storage) {
	s.store = store
}

func (s *Server) bootstrapPeerWrap(client adnl.Peer) error {
	s.bootstrapPeer(client)
	return nil
}

func (s *Server) GetPeerIfActive(id []byte) *PeerConnection {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.bootstrapped[hex.EncodeToString(id)]
}

func (s *Server) bootstrapPeer(client adnl.Peer) *PeerConnection {
	s.mx.Lock()
	defer s.mx.Unlock()

	if rl := s.bootstrapped[hex.EncodeToString(client.GetID())]; rl != nil {
		return rl
	}

	extADNL := overlay.CreateExtendedADNL(client)
	extADNL.SetOnUnknownOverlayQuery(s.handleQuery(extADNL))

	rl := overlay.CreateExtendedRLDP(rldp.NewClientV2(extADNL))
	rl.SetOnUnknownOverlayQuery(s.handleRLDPQuery(rl))

	rl.SetOnDisconnect(func() {
		s.mx.Lock()
		delete(s.bootstrapped, hex.EncodeToString(client.GetID()))
		s.mx.Unlock()
	})

	p := &PeerConnection{
		rldp:          rl,
		adnl:          client,
		usedByBags:    map[string]*storagePeer{},
		rldpQueue:     make(chan struct{}, 10),
		bagsInitQueue: make(chan struct{}, 8),
		failedBags:    map[string]bool{},
	}
	s.bootstrapped[hex.EncodeToString(client.GetID())] = p

	return p
}

func (s *Server) handleQuery(peer *overlay.ADNLWrapper) func(query *adnl.MessageQuery) error {
	return func(query *adnl.MessageQuery) error {
		req, over := overlay.UnwrapQuery(query.Data)

		if s.store == nil {
			return fmt.Errorf("storage is not yet initialized")
		}

		t := s.store.GetTorrentByOverlay(over)
		if t == nil {
			return fmt.Errorf("bag not found")
		}

		isDow, isUpl := t.IsActive()
		if !isUpl && !isDow {
			return fmt.Errorf("bag %s is not active", hex.EncodeToString(t.BagID))
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		switch q := req.(type) {
		case overlay.GetRandomPeers:
			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return err
			}

			peers := []overlay.Node{*node}
			t.peersMx.RLock()
			for _, nd := range t.knownNodes {
				peers = append(peers, *nd)
				if len(peers) == 8 {
					break
				}
			}
			t.peersMx.RUnlock()

			err = peer.Answer(ctx, query.ID, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case Ping:
			p := s.GetPeerIfActive(peer.GetID())
			if p == nil {
				return fmt.Errorf("peer disconnected")
			}

			stNode, sessionCtx := t.prepareStoragePeer(over, p, &q.SessionID)
			if sessionCtx != nil {
				go stNode.initializeSession(sessionCtx, atomic.LoadInt64(&stNode.sessionId), false)
			}
			stNode.touch()

			if err := peer.Answer(ctx, query.ID, Pong{}); err != nil {
				return err
			}
		}

		return nil
	}
}

func (s *Server) handleRLDPQuery(peer *overlay.RLDPWrapper) func(transfer []byte, query *rldp.Query) error {
	return func(transfer []byte, query *rldp.Query) error {
		req, over := overlay.UnwrapQuery(query.Data)

		if s.store == nil {
			return fmt.Errorf("storage is not yet initialized")
		}

		t := s.store.GetTorrentByOverlay(over)
		if t == nil {
			return fmt.Errorf("bag not found")
		}

		isDow, isUpl := t.IsActive()
		if !isDow && !isUpl {
			return fmt.Errorf("bag %s is not active", hex.EncodeToString(t.BagID))
		}

		adnlId := peer.GetADNL().GetID()

		var sesId *int64
		var timeout = 25 * time.Second
		switch q := req.(type) {
		case GetPiece:
			timeout = t.transmitTimeout()
		case Ping:
			sesId = &q.SessionID
		}

		p := s.GetPeerIfActive(adnlId)
		if p == nil {
			return fmt.Errorf("peer disconnected")
		}

		stPeer, sessionCtx := t.prepareStoragePeer(over, p, sesId)
		if sessionCtx != nil {
			go stPeer.initializeSession(sessionCtx, atomic.LoadInt64(&stPeer.sessionId), sesId == nil)
		}
		stPeer.touch()

		ctx, cancel := context.WithTimeout(t.globalCtx, timeout)
		defer cancel()

		switch q := req.(type) {
		case overlay.GetRandomPeers:
			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return err
			}

			peers := []overlay.Node{*node}
			t.peersMx.RLock()
			for _, nd := range t.knownNodes {
				peers = append(peers, *nd)
				if len(peers) == 8 {
					break
				}
			}
			t.peersMx.RUnlock()

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case GetPiece:
			if !isUpl {
				return fmt.Errorf("bag is not for upload")
			}

			tm := time.Now()

			err := t.GetConnector().ThrottleUpload(ctx, uint64(t.Info.PieceSize))
			if err != nil {
				// we will not respond on this, because it will be incompatible with protocol,
				// sender will retry to get the data
				return nil
			}

			pc, err := t.GetPiece(uint32(q.PieceID))
			if err != nil {
				return err
			}

			Logger("[STORAGE] LOADED PIECE", q.PieceID, hex.EncodeToString(adnlId), "TIME", time.Since(tm).String())

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, pc)
			if err != nil {
				Logger("[STORAGE] FAIL ANSWER PIECE", q.PieceID, hex.EncodeToString(adnlId), "TIME", time.Since(tm).String())

				return err
			}

			Logger("[STORAGE] SENT PIECE", q.PieceID, hex.EncodeToString(adnlId), "TIME", time.Since(tm).String())

			t.UpdateUploadedPeer(stPeer, uint64(len(pc.Data)))
		case Ping:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, Pong{})
			if err != nil {
				return err
			}
		case GetTorrentInfo:
			if !isUpl {
				return fmt.Errorf("bag is not for upload")
			}

			c, err := tlb.ToCell(t.Info)
			if err != nil {
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, TorrentInfoContainer{
				Data: c.ToBOC(),
			})
			if err != nil {
				return err
			}
		case AddUpdate:
			if q.SessionID != atomic.LoadInt64(&stPeer.sessionId) {
				Logger("UPDATE SESSION MISSMATCH", q.SessionID, atomic.LoadInt64(&stPeer.sessionId), hex.EncodeToString(adnlId), hex.EncodeToString(t.BagID))
				return fmt.Errorf("session id mismatch")
			}

			switch u := q.Update.(type) {
			case UpdateInit:
				atomic.StoreInt32(&stPeer.updateInitReceived, 1)

				Logger("[STORAGE] NODE REPORTED INIT PIECES INFO", hex.EncodeToString(adnlId), q.SessionID, q.Seqno)
				off := uint32(u.HavePiecesOffset)

				var piecesNum *uint32
				t.mx.RLock()
				if t.Info != nil {
					pn := t.Info.PiecesNum()
					piecesNum = &pn
				}
				t.mx.RUnlock()

				if piecesNum != nil && uint32(u.HavePiecesOffset)+uint32(len(u.HavePieces)) > *piecesNum { // approx is enough
					return fmt.Errorf("invalid pieces offset")
				}

				// TODO: bitmask
				stPeer.piecesMx.Lock()
				for i := 0; i < len(u.HavePieces); i++ {
					if u.HavePieces[i] == 0 {
						continue
					}

					for y := 0; y < 8; y++ {
						if u.HavePieces[i]&(1<<y) > 0 {
							stPeer.hasPieces[off+uint32(i*8+y)] = true
						}
					}
				}
				stPeer.piecesMx.Unlock()
			case UpdateHavePieces:
				var piecesNum *uint32
				t.mx.RLock()
				if t.Info != nil {
					pn := t.Info.PiecesNum()
					piecesNum = &pn
				}
				t.mx.RUnlock()

				Logger("[STORAGE] NODE HAS NEW PIECES", hex.EncodeToString(adnlId))
				stPeer.piecesMx.Lock()
				for _, d := range u.PieceIDs {
					if piecesNum != nil && uint32(d) > *piecesNum {
						// stop process it
						break
					}

					stPeer.hasPieces[uint32(d)] = true
				}
				stPeer.piecesMx.Unlock()
			}

			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, &Ok{})
			if err != nil {
				return err
			}
		}

		return nil
	}
}

const maxPiecesBytesPerRequest = 6000
const maxNewPiecesPerRequest = maxPiecesBytesPerRequest / 4

func (p *storagePeer) updateInitPieces(ctx context.Context) error {
	num := p.torrent.Info.PiecesNum()
	isDow, isUpl := p.torrent.IsActiveRaw()

	p.piecesMx.Lock()
	p.lastSentPieces = p.torrent.PiecesMask()
	p.piecesMx.Unlock()

	sent := uint32(0)
	for i := uint32(0); sent < num; i++ {
		p.piecesMx.RLock()
		have := p.lastSentPieces[i*maxPiecesBytesPerRequest:]
		p.piecesMx.RUnlock()

		if len(have) > maxPiecesBytesPerRequest {
			have = have[:maxPiecesBytesPerRequest]
		}

		up := AddUpdate{
			SessionID: atomic.LoadInt64(&p.sessionId),
			Seqno:     atomic.AddInt64(&p.sessionSeqno, 1),
			Update: UpdateInit{
				HavePieces:       have,
				HavePiecesOffset: int32(sent),
				State: State{
					WillUpload:   isUpl,
					WantDownload: isDow,
				},
			},
		}

		var updRes Ok

		if err := p.conn.AcquireQueueSlotWait(ctx); err != nil {
			return fmt.Errorf("failed to acquire queue slot: %w", err)
		}

		ctxReq, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &updRes)
		cancel()
		p.conn.FreeQueueSlot()
		if err != nil {
			Logger("[STORAGE] FAILED TO SEND UPDATE INIT", i, hex.EncodeToString(p.conn.adnl.GetID()), p.sessionId, err.Error())
			return fmt.Errorf("failed to send have pieces update: %w", err)
		}

		sent += uint32(len(have) * 8)
	}

	return nil
}

func (p *storagePeer) updateHavePieces(ctx context.Context) error {
	mask := p.torrent.PiecesMask()

	p.piecesMx.Lock()
	lastPieces := append([]byte{}, p.lastSentPieces...)

	var newPieces []int32
	for i := 0; i < len(mask); i++ {
		for j := 0; j < 8; j++ {
			if mask[i]&(1<<j) > 0 && (i >= len(lastPieces) || lastPieces[i]&(1<<j) == 0) {
				newPieces = append(newPieces, int32(i*8+j))
			}
		}
	}
	p.lastSentPieces = mask
	p.piecesMx.Unlock()

	sent := 0
	for i := 0; sent < len(newPieces); i++ {
		have := newPieces[i*maxNewPiecesPerRequest:]
		if len(have) > maxNewPiecesPerRequest {
			have = have[:maxNewPiecesPerRequest]
		}

		up := AddUpdate{
			SessionID: atomic.LoadInt64(&p.sessionId),
			Seqno:     atomic.AddInt64(&p.sessionSeqno, 1),
			Update: UpdateHavePieces{
				PieceIDs: have,
			},
		}

		var updRes Ok

		if err := p.conn.AcquireQueueSlot(); err != nil {
			return fmt.Errorf("failed to acquire queue slot: %w", err)
		}

		ctxReq, cancel := context.WithTimeout(ctx, 7*time.Second)
		err := p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &updRes)
		cancel()
		p.conn.FreeQueueSlot()
		if err != nil {
			Logger("[STORAGE] FAILED TO SEND UPDATE HAVE", i, hex.EncodeToString(p.conn.adnl.GetID()), p.sessionId, err.Error())
			return fmt.Errorf("failed to send have pieces update: %w", err)
		}

		sent += len(have)
	}

	return nil
}

func (p *storagePeer) updateState(ctx context.Context) error {
	up := AddUpdate{
		SessionID: atomic.LoadInt64(&p.sessionId),
		Seqno:     atomic.AddInt64(&p.sessionSeqno, 1),
		Update: UpdateState{
			State: State{
				WillUpload:   true,
				WantDownload: true,
			},
		},
	}

	if err := p.conn.AcquireQueueSlotWait(ctx); err != nil {
		return fmt.Errorf("failed to acquire queue slot: %w", err)
	}

	var res Ok
	ctxReq, cancel := context.WithTimeout(ctx, 7*time.Second)
	err := p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &res)
	cancel()
	p.conn.FreeQueueSlot()
	if err != nil {
		Logger("[STORAGE] FAILED TO SEND UPDATE STATE", hex.EncodeToString(p.conn.adnl.GetID()), err.Error())
		return fmt.Errorf("failed to send state update: %w", err)
	}
	return nil
}

func (s *Server) updateDHT(ctx context.Context) error {
	addr := s.gate.GetAddressList()

	ctxStore, cancel := context.WithTimeout(ctx, 90*time.Second)
	stored, id, err := s.dht.StoreAddress(ctxStore, addr, 20*time.Minute, s.key, 3)
	cancel()
	if err != nil && stored == 0 {
		return err
	}

	// make sure it was saved
	_, _, err = s.dht.FindAddresses(ctx, id)
	if err != nil {
		return err
	}

	Logger("[STORAGE_DHT] OUR NODE ADDRESS UPDATED ON", stored, "NODES")

	return nil
}

func (s *Server) checkAndUpdateBagDHT(ctx context.Context, torrent *Torrent, isServer bool) error {
	Logger("[STORAGE_DHT] CHECKING BAG OVERLAY FOR", hex.EncodeToString(torrent.BagID))

	tm := time.Now()
	nodesList, _, err := s.dht.FindOverlayNodes(ctx, torrent.BagID)
	if err != nil && !errors.Is(err, dht.ErrDHTValueIsNotFound) {
		Logger("[STORAGE_DHT] FAILED TO FIND DHT OVERLAY RECORD FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return err
	}

	if nodesList == nil {
		nodesList = &overlay.NodesList{}
	} else {
		for i := range nodesList.List {
			torrent.addNode(nodesList.List[i], func() *address.List {
				adnlID, _ := tl.Hash(nodesList.List[i].ID.(adnl.PublicKeyED25519))
				s.addrMx.RLock()
				addr := s.registeredAddrs[string(adnlID)]
				s.addrMx.RUnlock()
				return addr
			})
		}
	}

	Logger("[STORAGE_DHT] FOUND", len(nodesList.List), "OVERLAY NODES FOR", hex.EncodeToString(torrent.BagID), "TOOK", time.Since(tm).String())

	node, err := overlay.NewNode(torrent.BagID, s.key)
	if err != nil {
		Logger("[STORAGE_DHT] FAILED CREATE OVERLAY NODE FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return err
	}

	hasExpired := false
	refreshed := false
	tooEarly := false
	// refresh if already exists
	for i := range nodesList.List {
		id, ok := nodesList.List[i].ID.(keys.PublicKeyED25519)
		if ok && id.Key.Equal(node.ID.(keys.PublicKeyED25519).Key) {
			if nodesList.List[i].Version > int32(time.Now().Unix()-(3*60)) {
				tooEarly = true
			} else {
				nodesList.List[i] = *node
			}

			refreshed = true
			break
		}

		if nodesList.List[i].Version < int32(time.Now().Unix()-(30*60)) {
			hasExpired = true
		}
	}

	if !refreshed {
		// create if no records
		if len(nodesList.List) == 0 {
			nodesList = &overlay.NodesList{
				List: []overlay.Node{*node},
			}
			refreshed = true
		} else {
			if len(nodesList.List) >= 5 {
				// only allowed to replace when have public ip, or something expired in the list
				if isServer || hasExpired {
					sort.Slice(nodesList.List, func(i, j int) bool {
						return nodesList.List[i].Version < nodesList.List[j].Version
					})

					// replace oldest
					nodesList.List[0] = *node
					refreshed = true
				}
			} else {
				// add our node if < 5 in list
				nodesList.List = append(nodesList.List, *node)
				refreshed = true
			}
		}
	}

	if refreshed && !tooEarly {
		Logger("[STORAGE] STORING BAG DHT RECORD", hex.EncodeToString(torrent.BagID))

		tm = time.Now()
		ctxStore, cancel := context.WithTimeout(ctx, 120*time.Second)
		stored, _, err := s.dht.StoreOverlayNodes(ctxStore, torrent.BagID, nodesList, 45*time.Minute, 3)
		cancel()
		if err != nil && stored == 0 {
			Logger("[STORAGE_DHT] FAILED TO STORE DHT OVERLAY RECORD FOR", hex.EncodeToString(torrent.BagID), err.Error())
			return err
		}
		Logger("[STORAGE_DHT] BAG OVERLAY UPDATED ON", stored, "NODES FOR", hex.EncodeToString(torrent.BagID), "TOOK", time.Since(tm).String())
	}

	return nil
}

func (s *Server) Stop() {
	s.closer()
	return
}

func (t *Torrent) addNode(node overlay.Node, getRegisteredAddrs func() *address.List) {
	nodeId, err := tl.Hash(node.ID)
	if err != nil {
		return
	}

	if bytes.Equal(nodeId, t.connector.GetID()) {
		Logger("[STORAGE] SKIP OURSELF", hex.EncodeToString(nodeId))

		// skip ourself
		return
	}

	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	if t.knownNodes[hex.EncodeToString(nodeId)] == nil {
		Logger("[STORAGE] ADD KNOWN NODE ", hex.EncodeToString(nodeId), "for", hex.EncodeToString(t.BagID))
		t.knownNodes[hex.EncodeToString(nodeId)] = &node

		go func() {
			ctx, cancel := context.WithTimeout(t.globalCtx, 120*time.Second)
			defer cancel()
			var addrs *address.List
			if getRegisteredAddrs != nil {
				addrs = getRegisteredAddrs()
			}
			err := t.connector.ConnectToNode(ctx, t, &node, addrs)
			if err != nil {
				Logger("[STORAGE] FAILED TO CONNECT TO NODE", hex.EncodeToString(nodeId), "FOR", hex.EncodeToString(t.BagID), err.Error())
				return
			}
		}()
	} else {
		// Logger("[STORAGE] ALREADY KNOWN NODE", hex.EncodeToString(nodeId))
	}
}

func (s *Server) ConnectToNode(ctx context.Context, t *Torrent, node *overlay.Node, addrs *address.List) error {
	key, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return fmt.Errorf("invalid node type")
	}

	adnlID, err := tl.Hash(key)
	if err != nil {
		return fmt.Errorf("failed build adnl from node key: %w", err)
	}

	peer := s.GetPeerIfActive(adnlID)
	if peer == nil {
		start := time.Now()
		if addrs == nil {
			lcCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			addrs, _, err = s.dht.FindAddresses(lcCtx, adnlID)
			cancel()
			if err != nil {
				Logger("[STORAGE] NOT FOUND NODE ADDR OF", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID), "ERR", err.Error(), "TOOK", time.Since(start).String())
				return fmt.Errorf("failed to find node address: %w", err)
			}
		}
		Logger("[STORAGE] ADDR FOR NODE ", hex.EncodeToString(adnlID), "FOUND", addrs.Addresses[0].IP.String(), addrs.Addresses[0].Port, "PUBKEY", hex.EncodeToString(key.Key), "FOR", hex.EncodeToString(t.BagID), "ELAPSED", time.Since(start).Seconds())

		addr := addrs.Addresses[0].IP.String() + ":" + fmt.Sprint(addrs.Addresses[0].Port)
		s.addrMx.Lock()
		s.registeredAddrs[string(adnlID)] = addrs
		s.addrMx.Unlock()
		ax, err := s.gate.RegisterClient(addr, key.Key)
		if err != nil {
			return fmt.Errorf("failed to connnect to node: %w", err)
		}
		peer = s.bootstrapPeer(ax)
	} else {
		Logger("[STORAGE] HAS ALREADY ACTIVE PEER FOR NODE ", hex.EncodeToString(adnlID), "ADDR", peer.adnl.RemoteAddr(), "ADDING FOR", hex.EncodeToString(t.BagID))
	}

	stNode, sessionCtx := t.prepareStoragePeer(node.Overlay, peer, nil)

	select {
	case <-ctx.Done():
		stNode.Close()
		return ctx.Err()
	case peer.bagsInitQueue <- struct{}{}:
		defer func() { <-peer.bagsInitQueue }()
	}

	success := true
	if sessionCtx != nil {
		success = stNode.initializeSession(sessionCtx, atomic.LoadInt64(&stNode.sessionId), t.IsDownloadAll())
	}

	if success {
		stNode.touch()

		Logger("[STORAGE] PEER CONNECTED", hex.EncodeToString(adnlID), peer.adnl.RemoteAddr(), "FOR", hex.EncodeToString(t.BagID), "PING", atomic.LoadInt64(&stNode.currentPing), "MS")

		startAt := time.Now().Unix()
		for {
			rcv := atomic.LoadInt32(&stNode.updateInitReceived) == 1
			if !rcv && time.Now().Unix()-startAt > 45 {
				Logger("[STORAGE_PEERS] PEER", hex.EncodeToString(stNode.nodeId), "HAS NOT SENT UPDATE INIT, SOMETHING WRONG, CLOSING CONNECTION", "BAG", hex.EncodeToString(t.BagID))
				stNode.Close()
				return fmt.Errorf("peer has not sent update init")
			} else if rcv {
				Logger("[STORAGE_PEERS] PEER", hex.EncodeToString(stNode.nodeId), "SENT UPDATE INIT, CONNECTION IS OK, BAG", hex.EncodeToString(t.BagID))
				break
			}

			select {
			case <-ctx.Done():
				stNode.Close()
				return ctx.Err()
			case <-time.After(25 * time.Millisecond):
			}
		}
	}

	return nil
}

func (p *storagePeer) prepareTorrentInfo(ctx context.Context) error {
	p.prepareInfoMx.Lock() // to not request one peer in parallel
	defer p.prepareInfoMx.Unlock()

	p.torrent.mx.RLock()
	hasInfo := p.torrent.Info != nil
	p.torrent.mx.RUnlock()

	if !hasInfo {
		tm := time.Now()
		Logger("[STORAGE] REQUESTING TORRENT INFO FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(p.torrent.BagID))

		if err := p.conn.AcquireQueueSlotWait(ctx); err != nil {
			return fmt.Errorf("failed to acquire queue slot: %w", err)
		}

		var res TorrentInfoContainer
		infCtx, cancel := context.WithTimeout(p.closerCtx, 20*time.Second)
		err := p.conn.rldp.DoQuery(infCtx, 1<<25, overlay.WrapQuery(p.overlay, &GetTorrentInfo{}), &res)
		cancel()
		p.conn.FreeQueueSlot()
		if err != nil {
			Logger("[STORAGE] ERR ", err.Error(), " REQUESTING TORRENT INFO FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(p.torrent.BagID))
			return err
		}
		Logger("[STORAGE] GOT TORRENT INFO TOOK", time.Since(tm).String(), "FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(p.torrent.BagID))

		cl, err := cell.FromBOC(res.Data)
		if err != nil {
			return fmt.Errorf("failed to parse torrent info boc: %w", err)
		}

		if !bytes.Equal(cl.Hash(), p.torrent.BagID) {
			return fmt.Errorf("incorrect torrent info")
		}

		var info TorrentInfo
		err = tlb.LoadFromCell(&info, cl.BeginParse())
		if err != nil {
			return fmt.Errorf("invalid torrent info cell")
		}

		if info.PieceSize == 0 || info.HeaderSize == 0 {
			err = fmt.Errorf("incorrect torrent info sizes")
			return err
		}
		if info.HeaderSize > 20*1024*1024 {
			err = fmt.Errorf("too big header > 20 MB, looks dangerous")
			return err
		}
		if info.PieceSize < 4096 {
			err = fmt.Errorf("too small piece < 4 KB, cannot be handled")
			return err
		}
		if info.PieceSize > 8*1024*1024 {
			err = fmt.Errorf("too big piece > 8 MB, cannot be handled")
			return err
		}
		if info.PiecesNum() > 32000000 {
			err = fmt.Errorf("too many pieces > 32000000, cannot be handled")
			return err
		}

		p.torrent.mx.Lock()
		if p.torrent.Info == nil {
			p.torrent.Info = &info
		}
		p.torrent.mx.Unlock()

		p.torrent.InitMask()
	}
	return nil
}

func (s *Server) startPeerSearcher() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	updateSem := make(chan struct{}, s.dhtParallelism)
	updatePrioritySem := make(chan struct{}, s.dhtParallelism)

	for {
		select {
		case <-s.closeCtx.Done():
			return
		case <-ticker.C:
		}

		if s.store == nil {
			continue
		}

		for _, t := range s.store.GetAll() {
			download, upload := t.IsActiveRaw()
			if !download && !upload {
				continue
			}

			completed := t.IsCompleted()
			if !upload && completed {
				continue
			}

			t.peersMx.RLock()
			numPeers := len(t.peers)
			for _, p := range t.peers {
				p.downloadSpeed.calculate(p.Downloaded)
				p.uploadSpeed.calculate(p.Uploaded)
			}
			t.peersMx.RUnlock()

			if upload || !completed {
				at := t.lastDHTStore

				activeDownload := !completed && download
				if activeDownload {
					if numPeers > 0 { // otherwise retry search immediately
						at = at.Add(45 * time.Second)
					} else {
						at = at.Add(7 * time.Second)
					}
				} else {
					if numPeers > 0 {
						at = at.Add(10*time.Minute + time.Duration(rand.Intn(60000))*time.Millisecond)
					} else {
						if !s.serverMode {
							tt := 15 * time.Second

							tt *= time.Duration(atomic.LoadUint32(&t.searchesWithZeroPeersNum) + 1)

							if tt > 7*time.Minute {
								tt = 7 * time.Minute
							}

							at = at.Add(tt + time.Duration(rand.Intn(5000))*time.Millisecond)
						} else {
							at = at.Add(5*time.Minute + time.Duration(rand.Intn(30000))*time.Millisecond)
						}
					}
				}

				if time.Now().After(at) &&
					(t.lastDHTStore.IsZero() || atomic.LoadInt32(&t.lastDHTStoreFailed) != 0 || t.lastDHTStore.UnixNano() < atomic.LoadInt64(&t.lastDHTStoreCompletedAt)) {
					t.lastDHTStore = time.Now()

					atomic.StoreInt32(&t.lastDHTStoreFailed, 0)

					if numPeers == 0 {
						atomic.AddUint32(&t.searchesWithZeroPeersNum, 1)
					} else {
						atomic.StoreUint32(&t.searchesWithZeroPeersNum, 0)
					}

					prioritized := activeDownload
					if t.CreatedLocally && t.CreatedAt.Add(15*time.Minute).After(time.Now()) {
						// ignore queue to made just created torrents available faster
						prioritized = true
					}

					Logger("[STORAGE] TIME TO MAKE BAG DHT RECORD QUERY", hex.EncodeToString(t.BagID), "PRIORITIZED:", prioritized)

					func(t *Torrent) {
						isPr := false
						// TODO: fair queue
						if !prioritized {
							updateSem <- struct{}{}
						} else {
							select {
							case updateSem <- struct{}{}:
							case updatePrioritySem <- struct{}{}:
								isPr = true
							}
						}

						defer func() {
							if !isPr {
								<-updateSem
							} else {
								<-updatePrioritySem
							}
							atomic.StoreInt64(&t.lastDHTStoreCompletedAt, time.Now().UnixNano())
						}()
						tm := time.Now()

						ctx, cancel := context.WithTimeout(t.globalCtx, time.Duration(90)*time.Second)
						err := s.checkAndUpdateBagDHT(ctx, t, s.serverMode)
						cancel()
						if err != nil {
							atomic.StoreInt32(&t.lastDHTStoreFailed, 1)
							Logger("[STORAGE] DHT QUERY BAG ERR", hex.EncodeToString(t.BagID), err.Error(), "TOOK", time.Since(tm).String())
							return
						}
						Logger("[STORAGE] BAG DHT RECORD QUERY", hex.EncodeToString(t.BagID), "TOOK", time.Since(tm).String())
					}(t)
				}
			}
		}
	}
}

func (s *Server) GetID() []byte {
	return s.gate.GetID()
}

func (s *Server) GetADNLPrivateKey() ed25519.PrivateKey {
	return s.key
}

func (t *Torrent) prepareStoragePeer(over []byte, conn *PeerConnection, sessionId *int64) (*storagePeer, context.Context) {
	t.mx.Lock()
	defer t.mx.Unlock()

	if n := conn.GetFor(t.BagID); n != nil {
		if sessionId == nil || atomic.LoadInt64(&n.sessionId) == *sessionId {
			return n, nil
		}

		n.stopSession()

		Logger("[STORAGE] REINITIALIZE REQUEST FOR", hex.EncodeToString(n.nodeId), "BAG", hex.EncodeToString(t.BagID), "OLD SESSION", atomic.LoadInt64(&n.sessionId), "NEW SESSION", *sessionId)

		atomic.StoreInt64(&n.sessionInitAt, time.Now().Unix())
		atomic.StoreInt32(&n.sessionInitialized, 0)
		atomic.StoreInt32(&n.updateInitReceived, 0)
		atomic.StoreInt64(&n.sessionId, *sessionId)
		atomic.StoreInt64(&n.sessionSeqno, 0)

		var sessionCtx context.Context
		sessionCtx, n.stopSession = context.WithCancel(n.closerCtx)

		return n, sessionCtx
	}

	if sessionId == nil {
		v := rand.Int63()
		sessionId = &v
	}

	stNode := &storagePeer{
		torrent:          t,
		nodeAddr:         conn.adnl.RemoteAddr(),
		nodeId:           conn.adnl.GetID(),
		conn:             conn,
		sessionId:        *sessionId,
		sessionInitAt:    time.Now().Unix(),
		overlay:          over,
		maxInflightScore: 50,
		// TODO: bitmask
		hasPieces: map[uint32]bool{},
	}
	stNode.closerCtx, stNode.stop = context.WithCancel(t.globalCtx)

	var sessionCtx context.Context
	sessionCtx, stNode.stopSession = context.WithCancel(stNode.closerCtx)

	conn.UseFor(stNode)

	return stNode, sessionCtx
}

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
	key        ed25519.PrivateKey
	dht        *dht.Client
	gate       *adnl.Gateway
	store      Storage
	closeCtx   context.Context
	serverMode bool

	bootstrapped map[string]*PeerConnection
	mx           sync.RWMutex

	closer func()
}

func NewServer(dht *dht.Client, gate *adnl.Gateway, key ed25519.PrivateKey, serverMode bool) *Server {
	s := &Server{
		key:          key,
		dht:          dht,
		gate:         gate,
		bootstrapped: map[string]*PeerConnection{},
		serverMode:   serverMode,
	}
	s.closeCtx, s.closer = context.WithCancel(context.Background())
	s.gate.SetConnectionHandler(s.bootstrapPeerWrap)

	go s.startPeerSearcher()
	go s.runPeersMonitor()

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
		rldp:       rl,
		adnl:       client,
		usedByBags: map[string]*storagePeer{},
		failedBags: map[string]bool{},
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

		_, isUpl := t.IsActive()
		if !isUpl {
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
			stPeer := p.GetFor(t.BagID)

			if stPeer == nil {
				var sesId = rand.Int63()
				switch q := req.(type) {
				case Ping:
					sesId = q.SessionID
				}

				t.mx.Lock()
				stPeer = t.initStoragePeer(t.globalCtx, over, s, p, sesId)
				atomic.StoreInt64(&stPeer.sessionSeqno, 0)
				t.mx.Unlock()
			}
			stPeer.touch()

			if err := peer.Answer(ctx, query.ID, Pong{}); err != nil {
				return err
			}

			if stPeer.isNeedInitSession(t, q.SessionID) {
				go func() {
					if err := stPeer.initSession(t, q.SessionID); err != nil {
						Logger("[STORAGE] FAILED TO INIT SESSION", hex.EncodeToString(peer.GetID()), err.Error())
					}
				}()
			}
		}

		return nil
	}
}

func (p *storagePeer) isNeedInitSession(t *Torrent, sesId int64) bool {
	s := atomic.LoadInt64(&p.sessionId)
	t.mx.Lock()
	hasInfo := t.Info != nil
	t.mx.Unlock()

	return !hasInfo || s != sesId || s == 0
}

func (p *storagePeer) initSession(t *Torrent, sesId int64) error {
	if err := p.prepareTorrentInfo(t); err != nil {
		Logger("[STORAGE] PREPARE INFO FAILED", hex.EncodeToString(p.conn.adnl.GetID()), sesId)
		return fmt.Errorf("failed to prepare torrent info: %w", err)
	}

	if atomic.LoadInt64(&p.sessionId) != sesId {
		atomic.StoreInt64(&p.sessionId, sesId)
		atomic.StoreInt64(&p.sessionSeqno, 0)
		Logger("[STORAGE] NEW SESSION WITH", hex.EncodeToString(p.conn.adnl.GetID()), sesId)
	}

	if atomic.LoadInt64(&p.sessionSeqno) == 0 {
		p.piecesMx.Lock()
		p.lastSentPieces = t.PiecesMask()
		p.piecesMx.Unlock()

		if err := p.updateInitPieces(context.Background()); err != nil {
			return fmt.Errorf("failed to send init pieces: %w", err)
		}
	}

	return nil
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

		p := s.GetPeerIfActive(adnlId)
		if p == nil {
			return fmt.Errorf("peer disconnected")
		}
		stPeer := p.GetFor(t.BagID)

		if stPeer == nil {
			var sesId = rand.Int63()
			switch q := req.(type) {
			case Ping:
				sesId = q.SessionID
			}

			t.mx.Lock()
			stPeer = t.initStoragePeer(t.globalCtx, over, s, p, sesId)
			atomic.StoreInt64(&stPeer.sessionSeqno, 0)
			t.mx.Unlock()

			// prepare torrent info if needed
			go stPeer.prepareTorrentInfo(t)
		}
		stPeer.touch()

		var timeout = 7 * time.Second
		switch req.(type) {
		case GetPiece:
			timeout = t.transmitTimeout()
		}

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

			if stPeer.isNeedInitSession(t, q.SessionID) {
				go func() {
					if err := stPeer.initSession(t, q.SessionID); err != nil {
						Logger("[STORAGE] FAILED TO INIT SESSION", hex.EncodeToString(peer.RLDP.GetADNL().GetID()), err.Error())
					}
				}()
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
		case UpdateState:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transfer, Ok{})
			if err != nil {
				return err
			}
		case AddUpdate:
			switch u := q.Update.(type) {
			case UpdateInit:
				Logger("[STORAGE] NODE REPORTED PIECES INFO", hex.EncodeToString(adnlId), q.SessionID, q.Seqno)
				stPeer.piecesMx.Lock()
				off := uint32(u.HavePiecesOffset)
				for i := 0; i < len(u.HavePieces); i++ {
					for y := 0; y < 8; y++ {
						if u.HavePieces[i]&(1<<y) > 0 {
							stPeer.hasPieces[off+uint32(i*8+y)] = true
						}
					}
				}
				stPeer.piecesMx.Unlock()
			case UpdateHavePieces:
				Logger("[STORAGE] NODE HAS NEW PIECES", hex.EncodeToString(adnlId))
				stPeer.piecesMx.Lock()
				for _, d := range u.PieceIDs {
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
	isDow, isUpl := p.torrent.IsActive()

	sent := uint32(0)
	for i := uint32(0); sent < num; i++ {
		have := p.lastSentPieces[i*maxPiecesBytesPerRequest:]
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

		ctxReq, cancel := context.WithTimeout(ctx, 7*time.Second)
		err := p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &updRes)
		cancel()
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

		ctxReq, cancel := context.WithTimeout(ctx, 7*time.Second)
		err := p.conn.rldp.DoQuery(ctxReq, 1<<20, overlay.WrapQuery(p.overlay, up), &updRes)
		cancel()
		if err != nil {
			Logger("[STORAGE] FAILED TO SEND UPDATE HAVE", i, hex.EncodeToString(p.conn.adnl.GetID()), p.sessionId, err.Error())
			return fmt.Errorf("failed to send have pieces update: %w", err)
		}

		sent += len(have)
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

func (s *Server) updateTorrent(ctx context.Context, torrent *Torrent, isServer bool) error {
	Logger("[STORAGE_DHT] CHECKING BAG OVERLAY FOR", hex.EncodeToString(torrent.BagID))

	nodesList, _, err := s.dht.FindOverlayNodes(ctx, torrent.BagID)
	if err != nil && !errors.Is(err, dht.ErrDHTValueIsNotFound) {
		Logger("[STORAGE_DHT] FAILED TO FIND DHT OVERLAY RECORD FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return err
	}

	if nodesList == nil {
		nodesList = &overlay.NodesList{}
	}

	Logger("[STORAGE_DHT] FOUND", len(nodesList.List), "OVERLAY NODES FOR", hex.EncodeToString(torrent.BagID))

	node, err := overlay.NewNode(torrent.BagID, s.key)
	if err != nil {
		Logger("[STORAGE_DHT] FAILED CREATE OVERLAY NODE FOR", hex.EncodeToString(torrent.BagID), err.Error())
		return err
	}

	hasExpired := false
	refreshed := false
	// refresh if already exists
	for i := range nodesList.List {
		id, ok := nodesList.List[i].ID.(adnl.PublicKeyED25519)
		if ok && id.Key.Equal(node.ID.(adnl.PublicKeyED25519).Key) {
			nodesList.List[i] = *node
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

	if refreshed {
		ctxStore, cancel := context.WithTimeout(ctx, 120*time.Second)
		stored, _, err := s.dht.StoreOverlayNodes(ctxStore, torrent.BagID, nodesList, 30*time.Minute, 3)
		cancel()
		if err != nil && stored == 0 {
			Logger("[STORAGE_DHT] FAILED TO STORE DHT OVERLAY RECORD FOR", hex.EncodeToString(torrent.BagID), err.Error())
			return err
		}
		Logger("[STORAGE_DHT] BAG OVERLAY UPDATED ON", stored, "NODES FOR", hex.EncodeToString(torrent.BagID))
	}
	return nil
}

func (s *Server) Stop() {
	s.closer()
	return
}

func (s *Server) addTorrentNode(node *overlay.Node, t *Torrent) {
	nodeId, err := tl.Hash(node.ID)
	if err != nil {
		return
	}

	if bytes.Equal(nodeId, s.gate.GetID()) {
		Logger("[STORAGE] SKIP OURSELF", hex.EncodeToString(nodeId))

		// skip ourself
		return
	}

	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	if t.knownNodes[hex.EncodeToString(nodeId)] == nil {
		Logger("[STORAGE] ADD KNOWN NODE ", hex.EncodeToString(nodeId), "for", hex.EncodeToString(t.BagID))
		t.knownNodes[hex.EncodeToString(nodeId)] = node

		go s.nodeConnector(nodeId, t, node, 1)
	} else {
		Logger("[STORAGE] ALREADY KNOWN NODE", hex.EncodeToString(nodeId))
	}
}

func (s *Server) nodeConnector(adnlID []byte, t *Torrent, node *overlay.Node, attempt int) {
	onFail := func() {
		select {
		case <-t.globalCtx.Done():
			t.peersMx.Lock()
			delete(t.knownNodes, hex.EncodeToString(adnlID))
			t.peersMx.Unlock()
			Logger("[STORAGE] REMOVED PEER FROM KNOWN", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
			return
		case <-time.After(time.Duration(attempt*2) * time.Second):
			// reconnect
			go s.nodeConnector(adnlID, t, node, attempt+1)
			Logger("[STORAGE] TRYING TO RECONNECT TO", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
		}
	}

	scaleCtx, stopScale := context.WithTimeout(t.globalCtx, 120*time.Second)
	stNode, err := s.connectToNode(scaleCtx, t, adnlID, node, nil, nil)
	stopScale()
	if err != nil {
		onFail()
		return
	}

	go func() {
		select {
		case <-stNode.globalCtx.Done():
			onFail()
		}
	}()

	Logger("[STORAGE] ADDED PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
}

func (s *Server) ConnectToNode(ctx context.Context, t *Torrent, node *overlay.Node, addrList *address.List, nodeKey ed25519.PublicKey) error {
	adnlID, err := tl.Hash(adnl.PublicKeyED25519{Key: nodeKey})
	if err != nil {
		return fmt.Errorf("failed build adnl from node key: %w", err)
	}
	Logger("[STORAGE] CONNECTING WITH BOOTSTRAP", hex.EncodeToString(adnlID), adnlID, "FOR", hex.EncodeToString(t.BagID))
	_, err = s.connectToNode(ctx, t, adnlID, node, addrList, nodeKey)
	return err
}

func (s *Server) connectToNode(ctx context.Context, t *Torrent, adnlID []byte, node *overlay.Node, addrs *address.List, keyN ed25519.PublicKey) (*storagePeer, error) {
	peer := s.GetPeerIfActive(adnlID)
	if peer == nil {
		start := time.Now()
		if addrs == nil && keyN == nil {
			lcCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
			var err error
			addrs, keyN, err = s.dht.FindAddresses(lcCtx, adnlID)
			cancel()
			if err != nil {
				Logger("[STORAGE] NOT FOUND NODE ADDR OF", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID), "ERR", err.Error(), "TOOK", time.Since(start).String())
				return nil, fmt.Errorf("failed to find node address: %w", err)
			}
		}
		Logger("[STORAGE] ADDR FOR NODE ", hex.EncodeToString(adnlID), "FOUND", addrs.Addresses[0].IP.String(), addrs.Addresses[0].Port, "PUBKEY", hex.EncodeToString(keyN), "FOR", hex.EncodeToString(t.BagID), "ELAPSED", time.Since(start).Seconds())

		addr := addrs.Addresses[0].IP.String() + ":" + fmt.Sprint(addrs.Addresses[0].Port)

		ax, err := s.gate.RegisterClient(addr, keyN)
		if err != nil {
			return nil, fmt.Errorf("failed to connnect to node: %w", err)
		}
		peer = s.bootstrapPeer(ax)
	} else {
		Logger("[STORAGE] HAS ALREADY ACTIVE PEER FOR NODE ", hex.EncodeToString(adnlID), "FOUND", peer.adnl.RemoteAddr(), "FOR", hex.EncodeToString(t.BagID))
	}

	addr := peer.adnl.RemoteAddr()

	t.mx.Lock()
	stNode := t.initStoragePeer(t.globalCtx, node.Overlay, s, peer, rand.Int63())
	t.mx.Unlock()

	if err := stNode.prepareTorrentInfo(t); err != nil {
		stNode.Close()
	}

	Logger("[STORAGE] PEER PREPARED", hex.EncodeToString(adnlID), addr, "FOR", hex.EncodeToString(t.BagID))

	return stNode, nil
}

func (p *storagePeer) prepareTorrentInfo(t *Torrent) error {
	p.prepareInfoMx.Lock() // to not request one peer in parallel
	defer p.prepareInfoMx.Unlock()

	t.mx.Lock()
	hasInfo := t.Info != nil
	t.mx.Unlock()

	if !hasInfo {
		tm := time.Now()
		Logger("[STORAGE] REQUESTING TORRENT INFO FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(t.BagID))

		var res TorrentInfoContainer
		infCtx, cancel := context.WithTimeout(p.globalCtx, 20*time.Second)
		err := p.conn.rldp.DoQuery(infCtx, 1<<25, overlay.WrapQuery(p.overlay, &GetTorrentInfo{}), &res)
		cancel()
		if err != nil {
			Logger("[STORAGE] ERR ", err.Error(), " REQUESTING TORRENT INFO FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(t.BagID))
			return err
		}
		Logger("[STORAGE] GOT TORRENT INFO TOOK", time.Since(tm).String(), "FROM", hex.EncodeToString(p.nodeId), p.nodeAddr, "FOR", hex.EncodeToString(t.BagID))

		cl, err := cell.FromBOC(res.Data)
		if err != nil {
			return fmt.Errorf("failed to parse torrent info boc: %w", err)
		}

		if !bytes.Equal(cl.Hash(), t.BagID) {
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

		t.mx.Lock()
		if t.Info == nil {
			t.Info = &info
		}
		t.mx.Unlock()

		t.InitMask()
	}
	return nil
}

func (s *Server) startPeerSearcher() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	searchSem := make(chan struct{}, 3)
	updateSem := make(chan struct{}, 9)

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
			t.peersMx.RUnlock()

			if t.lastPeersSearch.IsZero() || t.lastPeersSearch.UnixNano() < atomic.LoadInt64(&t.lastPeersSearchCompletedAt) {
				at := t.lastPeersSearch.Add(30 * time.Second)
				if completed {
					at = at.Add(3 * time.Minute)
				}
				at = at.Add(time.Duration(rand.Intn(10000)) * time.Millisecond)

				if numPeers > 0 {
					at = at.Add(5 * time.Minute).Add(time.Duration(rand.Intn(60000)) * time.Millisecond)
				}

				if time.Now().After(at) {
					t.lastPeersSearch = time.Now()
					// force search peers if we want to download
					ignoreQueue := !completed && download

					go func(t *Torrent) {
						if !ignoreQueue {
							searchSem <- struct{}{}
						}

						defer func() {
							if !ignoreQueue {
								<-searchSem
							}
							atomic.StoreInt64(&t.lastPeersSearchCompletedAt, time.Now().UnixNano())
						}()

						Logger("[STORAGE] SEARCHING PEERS FOR", hex.EncodeToString(t.BagID))

						ctxFind, cancel := context.WithTimeout(t.globalCtx, time.Duration(60)*time.Second)
						nodes, _, err := s.dht.FindOverlayNodes(ctxFind, t.BagID, nil)
						cancel()
						if err != nil {
							Logger("[STORAGE] DHT SEARCH PEER ERR", hex.EncodeToString(t.BagID), err.Error())
							return
						}

						for i := range nodes.List {
							// add known nodes in case we will need them in future to scale
							s.addTorrentNode(&nodes.List[i], t)
						}
					}(t)
				}
			}

			if upload || !completed {
				at := t.lastDHTStore.Add(9 * time.Minute)
				at = at.Add(time.Duration(rand.Intn(60000)) * time.Millisecond)

				if time.Now().After(at) &&
					(t.lastDHTStore.IsZero() || t.lastDHTStore.UnixNano() < atomic.LoadInt64(&t.lastDHTStoreCompletedAt)) {
					t.lastDHTStore = time.Now()
					go func(t *Torrent) {
						updateSem <- struct{}{}

						defer func() {
							<-updateSem
							atomic.StoreInt64(&t.lastDHTStoreCompletedAt, time.Now().UnixNano())
						}()
						tm := time.Now()

						Logger("[STORAGE] STORING BAG DHT RECORD", hex.EncodeToString(t.BagID))

						ctx, cancel := context.WithTimeout(t.globalCtx, time.Duration(90)*time.Second)
						err := s.updateTorrent(ctx, t, s.serverMode)
						cancel()
						if err != nil {
							Logger("[STORAGE] DHT STORE BAG ERR", hex.EncodeToString(t.BagID), err.Error(), "TOOK", time.Since(tm).String())
							return
						}

						Logger("[STORAGE] BAG DHT RECORD UPDATED", hex.EncodeToString(t.BagID), "TOOK", time.Since(tm).String())
					}(t)
				}
			}
		}
	}
}

func (s *Server) GetADNLPrivateKey() ed25519.PrivateKey {
	return s.key
}

func (t *Torrent) initStoragePeer(globalCtx context.Context, over []byte, srv *Server, conn *PeerConnection, sessionId int64) *storagePeer {
	if n := conn.GetFor(t.BagID); n != nil {
		return n
	}

	stNode := &storagePeer{
		torrent:          t,
		nodeAddr:         conn.adnl.RemoteAddr(),
		nodeId:           conn.adnl.GetID(),
		conn:             conn,
		sessionId:        sessionId,
		overlay:          over,
		maxInflightScore: 50,
		hasPieces:        map[uint32]bool{},
	}

	conn.UseFor(stNode)
	stNode.globalCtx, stNode.stop = context.WithCancel(globalCtx)
	go stNode.pinger(srv)

	return stNode
}

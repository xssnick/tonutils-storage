package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	key      ed25519.PrivateKey
	dht      *dht.Client
	gate     *adnl.Gateway
	store    Storage
	closeCtx context.Context

	bootstrapped map[adnl.Peer]overlay.RLDP
	mx           sync.RWMutex

	closer func()
}

func NewServer(dht *dht.Client, gate *adnl.Gateway, key ed25519.PrivateKey, serverMode bool) *Server {
	s := &Server{
		key:          key,
		dht:          dht,
		gate:         gate,
		bootstrapped: map[adnl.Peer]overlay.RLDP{},
	}
	s.closeCtx, s.closer = context.WithCancel(context.Background())
	s.gate.SetConnectionHandler(s.bootstrapPeerWrap)

	if serverMode {
		go func() {
			wait := 1 * time.Second
			// refresh dht records
			for {
				select {
				case <-s.closeCtx.Done():
					return
				case <-time.After(wait):
				}

				ctx, cancel := context.WithTimeout(s.closeCtx, 100*time.Second)
				err := s.updateDHT(ctx)
				cancel()

				if err != nil {
					pterm.Warning.Println("DHT ADNL address record update failed: ", err, ". We will retry in 5 sec")

					// on err, retry sooner
					wait = 5 * time.Second
					continue
				}
				wait = 1 * time.Minute
			}
		}()

		go func() {
			wait := 3 * time.Second
			// refresh dht records
			for {
				select {
				case <-s.closeCtx.Done():
					return
				case <-time.After(wait):
				}

				ctx, cancel := context.WithTimeout(s.closeCtx, 100*time.Second)
				err := s.updateTorrents(ctx)
				if err == nil {
					wait = 1 * time.Minute
				} else {
					wait = 15 * time.Second
				}
				cancel()
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

func (s *Server) bootstrapPeer(client adnl.Peer) overlay.RLDP {
	s.mx.Lock()
	defer s.mx.Unlock()

	if rl := s.bootstrapped[client]; rl != nil {
		return rl
	}

	extADNL := overlay.CreateExtendedADNL(client)
	extADNL.SetOnUnknownOverlayQuery(s.handleQuery(extADNL))
	extADNL.SetDisconnectHandler(func(addr string, key ed25519.PublicKey) {
		s.mx.Lock()
		delete(s.bootstrapped, client)
		s.mx.Unlock()
	})

	rl := overlay.CreateExtendedRLDP(rldp.NewClientV2(extADNL))
	rl.SetOnUnknownOverlayQuery(s.handleRLDPQuery(rl))
	s.bootstrapped[client] = rl

	return rl
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
			return fmt.Errorf("bag is not active")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		println("GOT ADNL ", peer.RemoteAddr(), reflect.ValueOf(req).String())

		switch req.(type) {
		case overlay.GetRandomPeers:
			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return err
			}

			peers := []overlay.Node{*node}
			t.peersMx.Lock()
			for _, nd := range t.knownNodes {
				peers = append(peers, *nd)
				if len(peers) == 8 {
					break
				}
			}
			t.peersMx.Unlock()

			err = peer.Answer(ctx, query.ID, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case Ping:
			err := peer.Answer(ctx, query.ID, Pong{})
			if err != nil {
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
			return fmt.Errorf("bag is not active")
		}

		adnlId := peer.GetADNL().GetID()

		var stPeer *storagePeer

		t.mx.Lock()
		p := t.GetPeer(adnlId)
		if p == nil {
			stPeer = initStoragePeer(t.globalCtx, t, over, peer.GetADNL().(overlay.ADNL), peer, rand.Int63())
		} else {
			stPeer = p.peer
		}
		t.mx.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		switch q := req.(type) {
		case overlay.GetRandomPeers:
			println("GOT GET PEERS")

			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return err
			}

			peers := []overlay.Node{*node}
			t.peersMx.Lock()
			for _, nd := range t.knownNodes {
				peers = append(peers, *nd)
				if len(peers) == 8 {
					break
				}
			}
			t.peersMx.Unlock()

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case GetPiece:
			if !isUpl {
				return fmt.Errorf("bag is not for upload")
			}

			err := t.GetConnector().ThrottleUpload(ctx, uint64(t.Info.PieceSize))
			if err != nil {
				return err
			}

			p, err := t.GetPiece(uint32(q.PieceID))
			if err != nil {
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, p)
			if err != nil {
				return err
			}

			t.UpdateUploadedPeer(stPeer, uint64(len(p.Data)))
		case Ping:
			if stPeer.sessionId == 0 {
				println("SET SES ID PEER", q.SessionID)

				stPeer.sessionId = q.SessionID
			}

			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, Pong{})
			if err != nil {
				return err
			}

			stPeer.piecesMx.RLock()
			lastPieces := stPeer.lastSentPieces
			stPeer.piecesMx.RUnlock()

			if atomic.LoadInt64(&stPeer.sessionSeqno) == 0 {
				println("SEND UPD")

				stPeer.piecesMx.Lock()
				stPeer.lastSentPieces = t.PiecesMask()
				stPeer.piecesMx.Unlock()

				up := AddUpdate{
					SessionID: stPeer.sessionId,
					Seqno:     atomic.AddInt64(&stPeer.sessionSeqno, 1),
					Update: UpdateInit{
						HavePieces:       stPeer.lastSentPieces,
						HavePiecesOffset: 0,
						State: State{
							WillUpload:   true,
							WantDownload: true,
						},
					},
				}

				var updRes Ok
				err = peer.DoQuery(ctx, query.MaxAnswerSize, overlay.WrapQuery(over, up), &updRes)
				if err != nil {
					return err
				}
			} else if len(lastPieces) > 0 {
				mask := t.PiecesMask()
				var newPieces []int32
				for i := 0; i < len(lastPieces); i++ {
					for j := 0; j < 8; j++ {
						if mask[i]&(1<<j) > 0 && lastPieces[i]&(1<<j) == 0 {
							newPieces = append(newPieces, int32(i*8+j))
						}
					}
				}

				if len(newPieces) > 0 {
					stPeer.piecesMx.Lock()
					lastPieces = mask
					stPeer.piecesMx.Unlock()

					up := AddUpdate{
						SessionID: q.SessionID,
						Seqno:     atomic.AddInt64(&stPeer.sessionSeqno, 1),
						Update: UpdateHavePieces{
							PieceIDs: newPieces,
						},
					}

					var res Ok
					err = peer.DoQuery(ctx, query.MaxAnswerSize, overlay.WrapQuery(over, up), &res)
					if err != nil {
						return err
					}
				}
			}
		case GetTorrentInfo:
			if !isUpl {
				return fmt.Errorf("bag is not for upload")
			}

			println("GET INF RLDP")

			c, err := tlb.ToCell(t.Info)
			if err != nil {
				println("GET INF CELL ERR", err.Error())
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, TorrentInfoContainer{
				Data: c.ToBOC(),
			})
			if err != nil {
				println("SEND INF ERR", err.Error())

				return err
			}

			// TODO: reinit on adnl reinit with new session
			// reset seqno, consider it as reinit flow for now
			atomic.StoreInt64(&stPeer.sessionSeqno, 0)

			var res Pong
			err = peer.DoQuery(ctx, query.MaxAnswerSize, overlay.WrapQuery(over, Ping{
				SessionID: stPeer.sessionId,
			}), &res)
			if err != nil {
				return err
			}
		case UpdateState:
			println("GOT UPDATE STATE")
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, Ok{})
			if err != nil {
				return err
			}
		case AddUpdate:
			println("GOT ADD UPDATE")

			switch u := q.Update.(type) {
			case UpdateInit:
				Logger("[DOWNLOADER] NODE REPORTED PIECES INFO", hex.EncodeToString(adnlId), q.SessionID, q.Seqno)
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
				Logger("[DOWNLOADER] NODE HAS NEW PIECES", hex.EncodeToString(adnlId))
				stPeer.piecesMx.Lock()
				for _, d := range u.PieceIDs {
					stPeer.hasPieces[uint32(d)] = true
				}
				stPeer.piecesMx.Unlock()
			}
			// do nothing with this info for now, just ok
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, &Ok{})
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func (s *Server) updateDHT(ctx context.Context) error {
	addr := s.gate.GetAddressList()

	ctxStore, cancel := context.WithTimeout(ctx, 80*time.Second)
	stored, id, err := s.dht.StoreAddress(ctxStore, addr, 15*time.Minute, s.key, 5)
	cancel()
	if err != nil && stored == 0 {
		return err
	}

	// make sure it was saved
	_, _, err = s.dht.FindAddresses(ctx, id)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) updateTorrents(ctx context.Context) error {
	if s.store == nil {
		return fmt.Errorf("storage is not yet initialized")
	}

	list := s.store.GetAll()

	for _, torrent := range list {
		nodesList, _, err := s.dht.FindOverlayNodes(ctx, torrent.BagID)
		if err != nil && !errors.Is(err, dht.ErrDHTValueIsNotFound) {
			continue
		}

		if nodesList == nil {
			nodesList = &overlay.NodesList{}
		}

		node, err := overlay.NewNode(torrent.BagID, s.key)
		if err != nil {
			pterm.Warning.Printf("Failed to update DHT record for bag %s: %v", hex.EncodeToString(torrent.BagID), err)
			continue
		}

		refreshed := false
		// refresh if already exists
		for i := range nodesList.List {
			id, ok := nodesList.List[i].ID.(adnl.PublicKeyED25519)
			if ok && id.Key.Equal(node.ID.(adnl.PublicKeyED25519).Key) {
				nodesList.List[i] = *node
				refreshed = true
				break
			}
		}

		if !refreshed {
			// create if no records
			if len(nodesList.List) == 0 {
				nodesList = &overlay.NodesList{
					List: []overlay.Node{*node},
				}
			} else {
				if len(nodesList.List) >= 5 {
					sort.Slice(nodesList.List, func(i, j int) bool {
						return nodesList.List[i].Version < nodesList.List[j].Version
					})

					// TODO: store in diff dht node instead
					// replace oldest
					nodesList.List[0] = *node
				} else {
					// add our node if < 5 in list
					nodesList.List = append(nodesList.List, *node)
				}
			}
		}

		ctxStore, cancel := context.WithTimeout(ctx, 30*time.Second)
		stored, _, err := s.dht.StoreOverlayNodes(ctxStore, torrent.BagID, nodesList, 60*time.Minute, 5)
		cancel()
		if err != nil && stored == 0 {
			pterm.Warning.Printf("Failed to store DHT record for bag %s: %v", hex.EncodeToString(torrent.BagID), err)
			continue
		}

		//	log.Println("DHT ADNL address record for bag was refreshed successfully")
	}
	return nil
}

func (s *Server) Stop() {
	s.closer()
	return
}

func (s *Server) addTorrentNode(node *overlay.Node, t *Torrent) {
	nodeId, err := adnl.ToKeyID(node.ID)
	if err != nil {
		return
	}

	if bytes.Equal(nodeId, s.gate.GetID()) {
		// skip ourself
		return
	}

	t.peersMx.Lock()
	defer t.peersMx.Unlock()

	if t.knownNodes[hex.EncodeToString(nodeId)] == nil {
		Logger("[STORAGE] ADD KNOWN NODE ", hex.EncodeToString(nodeId), "for", hex.EncodeToString(t.BagID))
		t.knownNodes[hex.EncodeToString(nodeId)] = node

		go s.nodeConnector(nodeId, t, node, 1)
	}
}

func (s *Server) nodeConnector(adnlID []byte, t *Torrent, node *overlay.Node, attempt int) {
	if t.GetPeer(adnlID) != nil {
		return
	}

	onFail := func() {
		select {
		case <-t.globalCtx.Done():
			t.peersMx.Lock()
			delete(t.knownNodes, hex.EncodeToString(adnlID))
			t.peersMx.Unlock()
			return
		case <-time.After(time.Duration(attempt*2) * time.Second):
			// reconnect
			go s.nodeConnector(adnlID, t, node, attempt+1)
		}
	}

	scaleCtx, stopScale := context.WithTimeout(t.globalCtx, 120*time.Second)
	stNode, err := s.connectToNode(scaleCtx, t, adnlID, node)
	stopScale()
	if err != nil {
		onFail()
		return
	}

	go func() {
		select {
		case <-t.globalCtx.Done():
			return
		case <-stNode.globalCtx.Done():
			onFail()
		}
	}()

	go func() {
		reqPeersCtx, cancel := context.WithTimeout(t.globalCtx, 120*time.Second)
		defer cancel()

		Logger("[DOWNLOADER] REQUESTING NODES LIST OF PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
		var al overlay.NodesList
		err = stNode.rldp.DoQuery(reqPeersCtx, 1<<25, &overlay.GetRandomPeers{}, &al)
		if err != nil {
			return
		}

		for _, n := range al.List {
			// add known nodes in case we will need them in future to scale
			s.addTorrentNode(&n, t)
		}
	}()

	Logger("[DOWNLOADER] ADDED PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
}

func (s *Server) connectToNode(ctx context.Context, t *Torrent, adnlID []byte, node *overlay.Node) (*storagePeer, error) {
	lcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	addrs, keyN, err := s.dht.FindAddresses(lcCtx, adnlID)
	cancel()
	if err != nil {
		Logger("[DOWNLOADER] NOT FOUND NODE ADDR OF", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
		return nil, fmt.Errorf("failed to find node address: %w", err)
	}

	Logger("[DOWNLOADER] ADDR FOR NODE ", hex.EncodeToString(adnlID), "FOUND", addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.BagID))

	addr := addrs.Addresses[0].IP.String() + ":" + fmt.Sprint(addrs.Addresses[0].Port)

	ax, err := s.gate.RegisterClient(addr, keyN)
	if err != nil {
		return nil, fmt.Errorf("failed to connnect to node: %w", err)
	}

	rl := s.bootstrapPeer(ax)
	if err != nil {
		return nil, fmt.Errorf("failed to bootstrap node: %w", err)
	}
	stNode := initStoragePeer(t.globalCtx, t, node.Overlay, ax, rl, 0)

	err = func() error {
		tm := time.Now()
		Logger("[DOWNLOADER] REQUESTING TORRENT INFO FROM", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.BagID))

		var res TorrentInfoContainer
		infCtx, cancel := context.WithTimeout(stNode.globalCtx, 20*time.Second)
		err = stNode.rldp.DoQuery(infCtx, 1<<25, overlay.WrapQuery(stNode.overlay, &GetTorrentInfo{}), &res)
		cancel()
		if err != nil {
			Logger("[DOWNLOADER] ERR ", err.Error(), " REQUESTING TORRENT INFO FROM", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.BagID))
			return err
		}
		Logger("[DOWNLOADER] GOT TORRENT INFO TOOK", time.Since(tm).String(), "FROM", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.BagID))

		cl, err := cell.FromBOC(res.Data)
		if err != nil {
			return fmt.Errorf("failed to parse torrent info boc: %w", err)
		}

		if !bytes.Equal(cl.Hash(), t.BagID) {
			return fmt.Errorf("incorrect torrent info")
		}

		t.mx.Lock()
		defer t.mx.Unlock()

		if t.Info == nil {
			var info TorrentInfo
			err = tlb.LoadFromCell(&info, cl.BeginParse())
			if err != nil {
				t.mx.Unlock()
				ax.Close()
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
			if info.PieceSize > 64*1024*1024 {
				err = fmt.Errorf("too big piece > 64 MB, looks dangerous")
				return err
			}
			t.Info = &info
		}
		return nil
	}()
	if err != nil {
		stNode.Close()
	}

	Logger("[DOWNLOADER] PEER PREPARED", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.BagID))

	return stNode, nil
}

func (s *Server) StartPeerSearcher(t *Torrent) {
	var nodesDhtCont *dht.Continuation
	for {
		var err error
		var nodes *overlay.NodesList
		Logger("[DOWNLOADER] SEARCHING PEERS FOR", hex.EncodeToString(t.BagID))

		ctxFind, cancel := context.WithTimeout(t.globalCtx, time.Duration(45)*time.Second)
		nodes, nodesDhtCont, err = s.dht.FindOverlayNodes(ctxFind, t.BagID, nodesDhtCont)
		cancel()
		if err != nil {
			select {
			case <-t.globalCtx.Done():
				Logger("[DOWNLOADER] DHT CONTEXT CANCEL", hex.EncodeToString(t.BagID))
				return
			case <-time.After(1 * time.Second):
				nodesDhtCont = nil
				Logger("[DOWNLOADER] DHT RETRY", hex.EncodeToString(t.BagID))
				continue
			}
		}

		for i := range nodes.List {
			// add known nodes in case we will need them in future to scale
			s.addTorrentNode(&nodes.List[i], t)
		}

		if len(nodes.List) > 0 {
			select {
			case <-t.globalCtx.Done():
				return
			case <-time.After(60 * time.Second):
			}
		}
	}
}

func initStoragePeer(globalCtx context.Context, torrent *Torrent, overlay []byte, ax overlay.ADNL, rl overlay.RLDP, sessionId int64) *storagePeer {
	println("INIT PEER", sessionId)
	stNode := &storagePeer{
		torrent:    torrent,
		nodeAddr:   ax.RemoteAddr(),
		nodeId:     ax.GetID(),
		rawAdnl:    ax,
		rldp:       rl,
		sessionId:  sessionId,
		overlay:    overlay,
		hasPieces:  map[uint32]bool{},
		pieceQueue: make(chan *pieceRequest),
	}
	stNode.globalCtx, stNode.stop = context.WithCancel(globalCtx)
	go stNode.pinger()

	return stNode
}

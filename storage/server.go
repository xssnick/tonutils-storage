package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/ton-payment-network/pkg/payments"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math/big"
	"math/rand"
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

	bootstrapped map[string]*PeerConnection
	mx           sync.RWMutex

	ps *PaymentEngine

	closer func()
}

func NewServer(dht *dht.Client, gate *adnl.Gateway, key ed25519.PrivateKey, serverMode, seedMode bool, ps *PaymentEngine) *Server {
	s := &Server{
		key:          key,
		dht:          dht,
		gate:         gate,
		ps:           ps,
		bootstrapped: map[string]*PeerConnection{},
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
					Logger("[STORAGE_DHT] STOPPED DHT UPDATER")
					return
				case <-time.After(wait):
				}

				Logger("[STORAGE_DHT] UPDATING OUR ADDRESS RECORD...")

				ctx, cancel := context.WithTimeout(s.closeCtx, 100*time.Second)
				err := s.updateDHT(ctx)
				cancel()

				if err != nil {
					Logger("[STORAGE_DHT] FAILED TO UPDATE OUR ADDRESS RECORD", err.Error())

					// on err, retry sooner
					wait = 5 * time.Second
					continue
				}
				wait = 1 * time.Minute
			}
		}()
	}

	if serverMode || seedMode {
		go func() {
			lastUpdate := map[string]int64{}
			wait := 5 * time.Second
			// refresh dht records
			for {
				select {
				case <-s.closeCtx.Done():
					return
				case <-time.After(wait):
				}

				if s.store == nil {
					continue
				}

				list := s.store.GetAll()
				for _, torrent := range list {
					if !torrent.activeUpload {
						continue
					}

					lastAt := lastUpdate[hex.EncodeToString(torrent.BagID)]
					if lastAt+180 <= time.Now().Unix() {
						ctx, cancel := context.WithTimeout(s.closeCtx, 45*time.Second)
						_ = s.updateTorrent(ctx, torrent, serverMode)
						cancel()
						lastUpdate[hex.EncodeToString(torrent.BagID)] = time.Now().Unix()
					}
				}
			}
		}()
	}

	return s
}

func (s *Server) SetStorage(store Storage) {
	s.mx.Lock()
	defer s.mx.Unlock()

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
			return fmt.Errorf("bag is not active")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		switch req.(type) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, overlay.NodesList{List: peers})
			if err != nil {
				return err
			}
		case GetPieceV2:
			pc, err := t.GetPiece(uint32(q.PieceID))
			if err != nil {
				return err
			}

			if s.ps != nil && s.ps.uploadPricePerByte.Sign() > 0 {
				if q.Payment == nil {
					return fmt.Errorf("no payment received")
				}

				var payment payments.Payment
				if err = tlb.LoadFromCell(&payment, q.Payment.BeginParse()); err != nil {
					return fmt.Errorf("failed to parse payment: %w", err)
				}

				amount := new(big.Int).Mul(big.NewInt(int64(len(pc.Data))), s.ps.uploadPricePerByte)
				if err = s.ps.Charge(ctx, amount, payment.Key, payment.State); err != nil {
					return fmt.Errorf("failed to parse payment: %w", err)
				}
				_ = t.addEarned(amount)
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, pc)
			if err != nil {
				return err
			}

			t.UpdateUploadedPeer(stPeer, uint64(len(pc.Data)))
		case GetPiece:
			if !isUpl {
				return fmt.Errorf("bag is not for upload")
			}

			if s.ps != nil && s.ps.uploadPricePerByte.Sign() > 0 {
				return fmt.Errorf("this is paid bag")
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
			if atomic.LoadInt64(&stPeer.sessionId) != q.SessionID {
				atomic.StoreInt64(&stPeer.sessionId, q.SessionID)
				atomic.StoreInt64(&stPeer.sessionSeqno, 0)
				Logger("[STORAGE] NEW SESSION WITH", hex.EncodeToString(adnlId), q.SessionID)
			}

			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, Pong{})
			if err != nil {
				return err
			}

			stPeer.piecesMx.RLock()
			lastPieces := stPeer.lastSentPieces
			stPeer.piecesMx.RUnlock()

			if atomic.LoadInt64(&stPeer.sessionSeqno) == 0 {
				stPeer.piecesMx.Lock()
				stPeer.lastSentPieces = t.PiecesMask()
				stPeer.piecesMx.Unlock()

				up := AddUpdate{
					SessionID: atomic.LoadInt64(&stPeer.sessionId),
					Seqno:     atomic.AddInt64(&stPeer.sessionSeqno, 1),
					Update: UpdateInit{
						HavePieces:       stPeer.lastSentPieces,
						HavePiecesOffset: 0,
						State: State{
							WillUpload:   isUpl,
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
				for i := 0; i < len(mask); i++ {
					for j := 0; j < 8; j++ {
						if mask[i]&(1<<j) > 0 && (i >= len(lastPieces) || lastPieces[i]&(1<<j) == 0) {
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
		case GetTorrentInfoV2:
			if !isUpl {
				return fmt.Errorf("bag is not for upload")
			}

			c, err := tlb.ToCell(t.Info)
			if err != nil {
				return err
			}

			var pricePerByte uint64 = 0
			var channelKey []byte
			if s.ps != nil {
				pricePerByte = s.ps.uploadPricePerByte.Uint64()
				channelKey = s.ps.wallet.PrivateKey().Public().(ed25519.PublicKey)
			} else {
				channelKey = make([]byte, 32)
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, TorrentInfoContainerV2{
				Data:         c.ToBOC(),
				ChannelKey:   channelKey,
				PricePerByte: pricePerByte,
			})
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

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, TorrentInfoContainer{
				Data: c.ToBOC(),
			})
			if err != nil {
				return err
			}
		case UpdateState:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, Ok{})
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
	stored, id, err := s.dht.StoreAddress(ctxStore, addr, 10*time.Minute, s.key, 8)
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
		println(err.Error())
		return err
	}

	if nodesList == nil {
		nodesList = &overlay.NodesList{}
	}

	node, err := overlay.NewNode(torrent.BagID, s.key)
	if err != nil {
		pterm.Warning.Printf("Failed to update DHT record for bag %s: %v", hex.EncodeToString(torrent.BagID), err)
		return err
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
			refreshed = true
		} else {
			if len(nodesList.List) >= 5 {
				// only allowed to replace when have public ip
				if isServer {
					sort.Slice(nodesList.List, func(i, j int) bool {
						return nodesList.List[i].Version < nodesList.List[j].Version
					})

					// TODO: store in diff dht node instead
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
		ctxStore, cancel := context.WithTimeout(ctx, 45*time.Second)
		stored, _, err := s.dht.StoreOverlayNodes(ctxStore, torrent.BagID, nodesList, 60*time.Minute, 5)
		cancel()
		if err != nil && stored == 0 {
			pterm.Warning.Printf("Failed to store DHT record for bag %s: %v", hex.EncodeToString(torrent.BagID), err)
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
		case <-stNode.globalCtx.Done():
			onFail()
		}
	}()

	Logger("[STORAGE] ADDED PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
}

func (s *Server) connectToNode(ctx context.Context, t *Torrent, adnlID []byte, node *overlay.Node) (*storagePeer, error) {
	peer := s.GetPeerIfActive(adnlID)
	if peer == nil {
		lcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		addrs, keyN, err := s.dht.FindAddresses(lcCtx, adnlID)
		cancel()
		if err != nil {
			Logger("[STORAGE] NOT FOUND NODE ADDR OF", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.BagID))
			return nil, fmt.Errorf("failed to find node address: %w", err)
		}

		Logger("[STORAGE] ADDR FOR NODE ", hex.EncodeToString(adnlID), "FOUND", addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.BagID))

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

func (s *storagePeer) prepareTorrentInfo(t *Torrent) error {
	tm := time.Now()
	Logger("[STORAGE] REQUESTING TORRENT INFO V2 FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))

	var res TorrentInfoContainer
	var resV2 TorrentInfoContainerV2

	infCtx, cancel := context.WithTimeout(s.globalCtx, 7*time.Second)
	err := s.conn.rldp.DoQuery(infCtx, 1<<25, overlay.WrapQuery(s.overlay, &GetTorrentInfoV2{}), &resV2)
	cancel()
	if err != nil {
		Logger("[STORAGE] FALLBACK, REQUESTING TORRENT INFO V1 FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))

		infCtx, cancel = context.WithTimeout(s.globalCtx, 7*time.Second)
		err = s.conn.rldp.DoQuery(infCtx, 1<<25, overlay.WrapQuery(s.overlay, &GetTorrentInfo{}), &res)
		cancel()
		if err != nil {
			Logger("[STORAGE] ERR ", err.Error(), " REQUESTING TORRENT INFO FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))
			return err
		}
	} else {
		s.supportsV2 = true
		res.Data = resV2.Data
	}
	Logger("[STORAGE] GOT TORRENT INFO TOOK", time.Since(tm).String(), "FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))

	if t.Info == nil {
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
		if info.PieceSize > 64*1024*1024 {
			err = fmt.Errorf("too big piece > 64 MB, looks dangerous")
			return err
		}

		t.mx.Lock()
		t.Info = &info
		t.mx.Unlock()
	}

	if resV2.PricePerByte > 0 {
		capacity := new(big.Int).Mul(new(big.Int).SetUint64(t.Info.FileSize), new(big.Int).SetUint64(resV2.PricePerByte))

		if resV2.PricePerByte > s.ps.downloadMaxPricePerByte.Uint64() {
			err = fmt.Errorf("too high price per byte %d, max allowed %s", resV2.PricePerByte, s.ps.downloadMaxPricePerByte.String())
			Logger("[PAYMENTS] FAILED TO OPEN VIRTUAL CHANNEL:", err.Error(), ", FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))
			return err
		}

		s.vpc = &virtualPaymentChannel{
			// increase planned capacity by 10% in case of network issues
			capacity:     new(big.Int).Add(capacity, new(big.Int).Div(capacity, big.NewInt(10))),
			used:         big.NewInt(0),
			pricePerByte: new(big.Int).SetUint64(resV2.PricePerByte),
		}

		var fee *big.Int
		tmPc := time.Now()
		s.vpc.key, fee, err = s.ps.OpenVirtualChannel(context.Background(), s.vpc.capacity, resV2.ChannelKey)
		if err != nil {
			Logger("[PAYMENTS] FAILED TO OPEN VIRTUAL CHANNEL:", err.Error(), ", CAPACITY", tlb.FromNanoTON(s.vpc.capacity).String(), "TON", ", FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))
			err = fmt.Errorf("failed to open virtual channel: %w", err)
			return err
		}
		_ = s.torrent.addPaid(fee)

		Logger("[PAYMENTS] OPENED VIRTUAL CHANNEL, TOOK", time.Since(tmPc).String(), "FROM", hex.EncodeToString(s.nodeId), s.nodeAddr, "FOR", hex.EncodeToString(t.BagID))
	}

	return nil
}

func (s *Server) StartPeerSearcher(t *Torrent) {
	var nodesDhtCont *dht.Continuation
	for {
		var err error
		var nodes *overlay.NodesList
		Logger("[STORAGE] SEARCHING PEERS FOR", hex.EncodeToString(t.BagID))

		ctxFind, cancel := context.WithTimeout(t.globalCtx, time.Duration(45)*time.Second)
		nodes, nodesDhtCont, err = s.dht.FindOverlayNodes(ctxFind, t.BagID, nodesDhtCont)
		cancel()
		if err != nil {
			select {
			case <-t.globalCtx.Done():
				Logger("[STORAGE] DHT CONTEXT CANCEL", hex.EncodeToString(t.BagID))
				return
			case <-time.After(1 * time.Second):
				nodesDhtCont = nil
				Logger("[STORAGE] DHT RETRY", hex.EncodeToString(t.BagID))
				continue
			}
		}

		for i := range nodes.List {
			// add known nodes in case we will need them in future to scale
			s.addTorrentNode(&nodes.List[i], t)
		}

		wait := 5

		t.peersMx.RLock()
		if len(t.knownNodes) > 0 {
			// found nodes, long sleep
			wait = 180
		}
		t.peersMx.RUnlock()

		select {
		case <-t.globalCtx.Done():
			return
		case <-time.After(time.Duration(wait) * time.Second):
		}
	}
}

func (t *Torrent) initStoragePeer(globalCtx context.Context, overlay []byte, srv *Server, conn *PeerConnection, sessionId int64) *storagePeer {
	if n := conn.GetFor(t.BagID); n != nil {
		return n
	}

	stNode := &storagePeer{
		torrent:    t,
		ps:         srv.ps,
		nodeAddr:   conn.adnl.RemoteAddr(),
		nodeId:     conn.adnl.GetID(),
		conn:       conn,
		sessionId:  sessionId,
		overlay:    overlay,
		hasPieces:  map[uint32]bool{},
		pieceQueue: make(chan *pieceRequest),
	}

	conn.UseFor(stNode)
	stNode.globalCtx, stNode.stop = context.WithCancel(globalCtx)
	go stNode.pinger(srv)

	return stNode
}

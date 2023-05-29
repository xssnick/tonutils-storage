package server

import (
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
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/storage"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Server struct {
	key      ed25519.PrivateKey
	dht      *dht.Client
	gate     *adnl.Gateway
	store    *db.Storage
	closeCtx context.Context

	bytesLimitPerSec  uint64
	uploaded          uint64
	uploadedUpdatedAt time.Time
	speedMx           sync.Mutex

	closer func()
}

func NewServer(store *db.Storage, dht *dht.Client, gate *adnl.Gateway, key ed25519.PrivateKey, serverMode bool) error {
	s := Server{
		key:   key,
		dht:   dht,
		gate:  gate,
		store: store,
	}
	s.closeCtx, s.closer = context.WithCancel(context.Background())

	s.gate.SetConnectionHandler(func(client adnl.Peer) error {
		session := rand.Int63()

		extADNL := overlay.CreateExtendedADNL(client)
		extADNL.SetOnUnknownOverlayQuery(s.handleQuery(extADNL, session))

		rl := overlay.CreateExtendedRLDP(rldp.NewClientV2(extADNL))
		rl.SetOnUnknownOverlayQuery(s.handleRLDPQuery(rl, session))

		return nil
	})

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
			wait := 1 * time.Second
			// refresh dht records
			for {
				select {
				case <-s.closeCtx.Done():
					return
				case <-time.After(wait):
				}

				ctx, cancel := context.WithTimeout(s.closeCtx, 100*time.Second)
				_ = s.updateTorrents(ctx)
				cancel()
				wait = 1 * time.Minute
			}
		}()
	}

	return nil
}

func (s *Server) SetSpeedLimit(bytesPerSec uint64) {
	s.bytesLimitPerSec = bytesPerSec
}

func (s *Server) handleQuery(peer *overlay.ADNLWrapper, session int64) func(query *adnl.MessageQuery) error {
	return func(query *adnl.MessageQuery) error {
		req, over := overlay.UnwrapQuery(query.Data)

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

			err = peer.Answer(ctx, query.ID, overlay.NodesList{List: []overlay.Node{*node}})
			if err != nil {
				return err
			}
		case storage.Ping:
			err := peer.Answer(ctx, query.ID, storage.Pong{})
			if err != nil {
				return err
			}
		case storage.GetTorrentInfo:
			c, err := tlb.ToCell(t.Info)
			if err != nil {
				return err
			}

			err = peer.Answer(ctx, query.ID, storage.TorrentInfoContainer{
				Data: c.ToBOC(),
			})
			if err != nil {
				return err
			}

			var res storage.Pong
			err = peer.Query(ctx, overlay.WrapQuery(over, storage.Ping{
				SessionID: session,
			}), &res)
			if err != nil {
				return err
			}
		case storage.AddUpdate:
			err := peer.Answer(ctx, query.ID, storage.Ok{})
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func (s *Server) handleRLDPQuery(peer *overlay.RLDPWrapper, session int64) func(transfer []byte, query *rldp.Query) error {
	isFirst := true
	return func(transfer []byte, query *rldp.Query) error {
		req, over := overlay.UnwrapQuery(query.Data)

		t := s.store.GetTorrentByOverlay(over)
		if t == nil {
			return fmt.Errorf("bag not found")
		}

		_, isUpl := t.IsActive()
		if !isUpl {
			return fmt.Errorf("bag is not active")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		updatePeer := true

		switch q := req.(type) {
		case overlay.GetRandomPeers:
			node, err := overlay.NewNode(t.BagID, s.key)
			if err != nil {
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, overlay.NodesList{List: []overlay.Node{*node}})
			if err != nil {
				return err
			}
		case storage.GetPiece:
			if s.bytesLimitPerSec > 0 {
				limitCtx, cancelLimit := context.WithTimeout(ctx, time.Duration(query.Timeout)*time.Second)
				defer cancelLimit()

				s.speedMx.Lock()
				select {
				case <-limitCtx.Done():
					s.speedMx.Unlock()
					// piece is not needed anymore
					return nil
				default:
				}

				if s.uploaded > s.bytesLimitPerSec {
					wait := time.Duration(float64(s.uploaded) / float64(s.bytesLimitPerSec) * float64(time.Second))

					select {
					case <-limitCtx.Done():
						s.speedMx.Unlock()
						// piece is not needed anymore
						return nil
					case <-time.After(wait):
						s.uploaded = 0
					}
				}
				s.uploadedUpdatedAt = time.Now()
				s.uploaded += uint64(t.Info.PieceSize)
				s.speedMx.Unlock()
			}

			p, err := t.GetPiece(uint32(q.PieceID))
			if err != nil {
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, p)
			if err != nil {
				return err
			}

			t.UpdateUploadedPeer(peer.GetADNL().GetID(), uint64(len(p.Data)))
			updatePeer = false
		case storage.Ping:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, storage.Pong{})
			if err != nil {
				return err
			}

			if isFirst {
				isFirst = false
				up := storage.AddUpdate{
					SessionID: q.SessionID,
					Seqno:     1,
					Update: storage.UpdateInit{
						HavePieces:       t.PiecesMask(),
						HavePiecesOffset: 0,
						State: storage.State{
							WillUpload:   true,
							WantDownload: true,
						},
					},
				}
				var res storage.Ok
				err = peer.DoQuery(ctx, query.MaxAnswerSize, overlay.WrapQuery(over, up), &res)
				if err != nil {
					return err
				}
			}
		case storage.GetTorrentInfo:
			c, err := tlb.ToCell(t.Info)
			if err != nil {
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, storage.TorrentInfoContainer{
				Data: c.ToBOC(),
			})
			if err != nil {
				return err
			}

			var res storage.Pong
			err = peer.DoQuery(ctx, query.MaxAnswerSize, overlay.WrapQuery(over, storage.Ping{
				SessionID: session,
			}), &res)
			if err != nil {
				return err
			}
		case storage.UpdateState:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, storage.Ok{})
			if err != nil {
				return err
			}
		case storage.AddUpdate:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, storage.Ok{})
			if err != nil {
				return err
			}
		default:
			updatePeer = false
		}

		if updatePeer {
			t.TouchPeer(peer.GetADNL().GetID())
		}

		return nil
	}
}

func (s *Server) updateDHT(ctx context.Context) error {
	addr := s.gate.GetAddressList()

	ctxStore, cancel := context.WithTimeout(ctx, 80*time.Second)
	stored, id, err := s.dht.StoreAddress(ctxStore, addr, 5*time.Minute, s.key, 5)
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

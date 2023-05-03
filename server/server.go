package server

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/adnl/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-storage/db"
	"log"
	"math/rand"
	"net"
	"reflect"
	"time"
)

type Server struct {
	key      ed25519.PrivateKey
	dht      *dht.Client
	gate     *adnl.Gateway
	store    *db.Storage
	closeCtx context.Context
	closer   func()
}

func NewServer(store *db.Storage, dht *dht.Client, key ed25519.PrivateKey, listen string, externalIP net.IP) error {
	gate := adnl.NewGateway(key)
	serverMode := externalIP != nil

	s := Server{
		key:   key,
		dht:   dht,
		gate:  gate,
		store: store,
	}
	s.closeCtx, s.closer = context.WithCancel(context.Background())

	if serverMode {
		gate.SetExternalIP(externalIP)
		err := s.gate.StartServer(listen)
		if err != nil {
			return err
		}
	} else {
		err := s.gate.StartClient()
		if err != nil {
			return err
		}
	}

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
					println("DHT ADNL address record update failed: ", err, ". We will retry in 5 sec")

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

func (s *Server) handleQuery(peer *overlay.ADNLWrapper, session int64) func(query *adnl.MessageQuery) error {
	return func(query *adnl.MessageQuery) error {
		req, over := overlay.UnwrapQuery(query.Data)

		t := s.store.GetTorrentByOverlay(over)
		if t == nil {
			return fmt.Errorf("torrent not found")
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
			return fmt.Errorf("torrent not found")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

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
			p, err := t.GetPiece(uint32(q.PieceID))
			if err != nil {
				println("ERR", err.Error())
				return err
			}

			err = peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, p)
			if err != nil {
				return err
			}

		case storage.Ping:
			err := peer.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transfer, storage.Pong{})
			if err != nil {
				return err
			}

			if isFirst {
				isFirst = false
				maskLen := len(t.PieceIndex) / 8
				if len(t.PieceIndex)%8 != 0 {
					maskLen++
				}

				bitmask := make([]byte, maskLen)
				for i := 0; i < maskLen; i++ {
					bitmask[i] = 0xFF
				}

				ppc := make([]int32, len(t.PieceIndex))
				for i := range t.PieceIndex {
					ppc[i] = int32(i)
				}

				up := storage.AddUpdate{
					SessionID: q.SessionID,
					Seqno:     1,
					Update: storage.UpdateInit{
						HavePieces:       bitmask,
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
			println("UNEXPECTED ", reflect.TypeOf(q).String())
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
		node, err := overlay.NewNode(torrent.BagID, s.key)
		if err != nil {
			log.Printf("Failed to update DHT record for torrent %s: %v", hex.EncodeToString(torrent.BagID), err)
			continue
		}
		nodesList := &overlay.NodesList{
			List: []overlay.Node{*node},
		}

		ctxStore, cancel := context.WithTimeout(ctx, 30*time.Second)
		stored, _, err := s.dht.StoreOverlayNodes(ctxStore, torrent.BagID, nodesList, 60*time.Minute, 5)
		cancel()
		if err != nil && stored == 0 {
			return err
		}

		//	log.Println("DHT ADNL address record for bag was refreshed successfully")
	}
	return nil
}

func (s *Server) Stop() {
	s.closer()
	return
}

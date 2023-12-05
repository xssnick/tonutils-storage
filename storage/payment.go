package storage

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/xssnick/ton-payment-network/pkg/payments"
	"github.com/xssnick/ton-payment-network/tonpayments"
	"github.com/xssnick/ton-payment-network/tonpayments/chain"
	"github.com/xssnick/ton-payment-network/tonpayments/db"
	"github.com/xssnick/ton-payment-network/tonpayments/db/leveldb"
	"github.com/xssnick/ton-payment-network/tonpayments/transport"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"math/big"
	"sync"
	"time"
)

type PaymentEngine struct {
	activeChannels map[string]*PaymentState

	svc *tonpayments.Service
	mx  sync.Mutex

	// move to torrent
	uploadPricePerByte      *big.Int
	downloadMaxPricePerByte *big.Int

	nodeFee     *big.Int
	nodeKey     ed25519.PublicKey
	wallet      *wallet.Wallet
	ctxStopper  context.Context
	stopper     func()
	closedEvent chan bool
}

type PaymentState struct {
	Key      []byte
	Deadline time.Time
	Capacity *big.Int
	Used     *big.Int

	LastUsed time.Time
}

func NewPaymentService(api ton.APIClientWrapped, gate *adnl.Gateway, dhtClient *dht.Client, wl *wallet.Wallet, nodeKey ed25519.PublicKey, nodeFee *big.Int) (*PaymentEngine, error) {
	log.Logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger().Level(zerolog.InfoLevel)
	log.Info().Str("node", hex.EncodeToString(nodeKey)).
		Hex("our", wl.PrivateKey().Public().(ed25519.PublicKey)).
		Msg("preparing payments engine")

	svc, err := prepare(api, gate, dhtClient, wl)
	if err != nil {
		return nil, err
	}

	ctx, stopper := context.WithCancel(context.Background())
	ps := &PaymentEngine{
		activeChannels:          map[string]*PaymentState{},
		svc:                     svc,
		closedEvent:             make(chan bool, 1),
		nodeFee:                 nodeFee,
		nodeKey:                 nodeKey,
		wallet:                  wl,
		ctxStopper:              ctx,
		stopper:                 stopper,
		uploadPricePerByte:      big.NewInt(0),
		downloadMaxPricePerByte: big.NewInt(0),
	}

	go ps.controller()

	log.Info().Str("node", hex.EncodeToString(nodeKey)).
		Hex("our", wl.PrivateKey().Public().(ed25519.PublicKey)).
		Msg("payments engine is ready")

	return ps, nil
}

func (s *PaymentEngine) SetPricePerByte(upload, downloadMax uint64) {
	s.uploadPricePerByte = new(big.Int).SetUint64(upload)
	s.downloadMaxPricePerByte = new(big.Int).SetUint64(downloadMax)
}

func (s *PaymentEngine) Close() {
	s.stopper()
	<-s.closedEvent
}

func (s *PaymentEngine) checkChannels() error {
	var in, out = big.NewInt(0), big.NewInt(0)

	channels, err := s.svc.GetChannelsWithNode(context.Background(), s.nodeKey)
	if err != nil {
		return fmt.Errorf("failed to get active channels: %w", err)
	}

	for _, ch := range channels {
		if ch.Status != db.ChannelStateActive {
			continue
		}

		their, err := ch.CalcBalance(true)
		if err != nil {
			return fmt.Errorf("failed to calc their balance on channel %s: %w", ch.Address, err)
		}

		our, err := ch.CalcBalance(false)
		if err != nil {
			return fmt.Errorf("failed to calc our balance on channel %s: %w", ch.Address, err)
		}

		log.Info().Str("address", ch.Address).
			Str("out_balance", tlb.FromNanoTON(our).String()).
			Str("in_balance", tlb.FromNanoTON(their).String()).
			Msg("found active channel")

		// exhausted := (their.Cmp(tlb.MustFromTON("0.3").Nano()) == -1 && our.Sign() == 0) ||
		//	(our.Cmp(tlb.MustFromTON("0.3").Nano()) == -1 && their.Sign() == 0)

		//if their.Cmp(tlb.MustFromTON("0.3").Nano()) == -1 {
		//	go svc.RequestCooperativeClose(context.Background(), ch.Address)
		// } else {
		in = in.Add(in, their)
		out = out.Add(out, our)
		// }
	}

	log.Info().Int("channels", len(channels)).Str("out_balance", tlb.FromNanoTON(out).String()).
		Str("in_balance", tlb.FromNanoTON(in).String()).
		Msg("payment channel balances")

	if in.Cmp(tlb.MustFromTON("1").Nano()) == -1 {
		log.Info().Str("node", hex.EncodeToString(s.nodeKey)).
			Str("balance", tlb.FromNanoTON(in).String()).
			Msg("not enough 'in' balance, requesting new channel with node")

		if err = s.svc.RequestInboundChannel(context.Background(), tlb.MustFromTON("3"), s.nodeKey); err != nil {
			return fmt.Errorf("failed to request inbound channel: %w", err)
		}
	}

	if out.Cmp(tlb.MustFromTON("1").Nano()) == -1 {
		log.Info().Str("node", hex.EncodeToString(s.nodeKey)).
			Str("balance", tlb.FromNanoTON(out).String()).
			Msg("not enough 'out' balance, deploying new channel with node")

		if _, err = s.svc.DeployChannelWithNode(context.Background(), tlb.MustFromTON("3"), s.nodeKey); err != nil {
			return fmt.Errorf("failed to deploy channel: %w", err)
		}
	}

	return nil
}

func (s *PaymentEngine) controller() {
	i := 0
	for {
		select {
		case <-s.ctxStopper.Done():
			if err := func() error {
				s.mx.Lock()
				defer s.mx.Unlock()

				channels, err := s.svc.GetChannelsWithNode(context.Background(), s.nodeKey)
				if err != nil {
					return fmt.Errorf("failed to get channels with node: %w", err)
				}

				for _, channel := range channels {
					for _, kv := range channel.Their.State.Data.Conditionals.All() {
						vc, err := payments.ParseVirtualChannelCond(kv.Value)
						if err != nil {
							return fmt.Errorf("failed to parse virtual channel condition: %w", err)
						}
						key := string(vc.Key)

						if err := s.svc.CloseVirtualChannel(context.Background(), vc.Key); err != nil {
							if errors.Is(err, tonpayments.ErrNoResolveExists) {
								continue
							}

							log.Warn().Err(err).Hex("key", vc.Key).Msg("failed to close virtual channel")
							continue
						}

						delete(s.activeChannels, key)
						log.Info().Hex("key", vc.Key).Msg("virtual channel closed")
					}
				}

				return nil
			}(); err != nil {
				log.Warn().Err(err).Msg("failed to close virtual channels")
			}
			close(s.closedEvent)
			return
		case <-time.After(3 * time.Second):
			if i%60 == 0 {
				if err := s.checkChannels(); err != nil {
					log.Warn().Err(err).Msg("payment service controller failed to check channels, will be repeated")
					continue
				}
			}

			i++
		}

		err := func() error {
			s.mx.Lock()
			defer s.mx.Unlock()

			channels, err := s.svc.GetChannelsWithNode(context.Background(), s.nodeKey)
			if err != nil {
				return fmt.Errorf("failed to get channels with node: %w", err)
			}

			for _, channel := range channels {
				for _, kv := range channel.Their.State.Data.Conditionals.All() {
					vc, err := payments.ParseVirtualChannelCond(kv.Value)
					if err != nil {
						return fmt.Errorf("failed to parse virtual channel condition: %w", err)
					}
					key := string(vc.Key)

					pc := s.activeChannels[key]
					if pc == nil {
						continue
					}

					if pc.Used.Cmp(pc.Capacity) == 0 ||
						time.Until(pc.Deadline) < 5*time.Minute ||
						time.Since(pc.LastUsed) > 3*time.Minute {
						if err := s.svc.CloseVirtualChannel(context.Background(), vc.Key); err != nil {
							if errors.Is(err, tonpayments.ErrNoResolveExists) {
								continue
							}

							log.Warn().Err(err).Hex("key", vc.Key).Msg("failed to close virtual channel")
							continue
						}

						delete(s.activeChannels, key)
						log.Info().Hex("key", vc.Key).Msg("virtual channel closed")
					}
				}
			}

			return nil
		}()
		if err != nil {
			log.Warn().Err(err).Msg("payment service controller loop error, will be repeated")
		}
	}
}

func prepare(api ton.APIClientWrapped, gate *adnl.Gateway, dhtClient *dht.Client, wl *wallet.Wallet) (*tonpayments.Service, error) {
	fdb, err := leveldb.NewDB("./tonutils-storage-db/payments/" + hex.EncodeToString(wl.PrivateKey().Public().(ed25519.PublicKey)))
	if err != nil {
		return nil, fmt.Errorf("failed to init leveldb: %w", err)
	}

	tr := transport.NewServer(dhtClient, gate, nil, wl.PrivateKey(), false)

	var seqno uint32
	if bo, err := fdb.GetBlockOffset(context.Background()); err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("failed to load block offset: %w", err)
		}
	} else {
		seqno = bo.Seqno
	}

	inv := make(chan any)
	sc := chain.NewScanner(api, payments.AsyncPaymentChannelCodeHash, seqno)
	log.Info().Uint32("from_seqno", seqno).Msg("starting chain scanner")

	go func() {
		if err := sc.Start(context.Background(), inv); err != nil {
			log.Error().Err(err).Msg("chain scanner failed")
		}
	}()

	log.Info().Str("addr", wl.WalletAddress().String()).Msg("wallet initialized")

	svc := tonpayments.NewService(api, fdb, tr, wl, inv, wl.PrivateKey(), payments.ClosingConfig{
		QuarantineDuration:       600,
		MisbehaviorFine:          tlb.MustFromTON("0.015"),
		ConditionalCloseDuration: 180,
	})
	tr.SetService(svc)
	go svc.Start()

	return svc, nil
}

func (s *PaymentEngine) Charge(ctx context.Context, amount *big.Int, key ed25519.PublicKey, state payments.VirtualChannelState) error {
	if err := s.svc.AddVirtualChannelResolve(ctx, key, state); err != nil && !errors.Is(err, db.ErrNewerStateIsKnown) {
		return fmt.Errorf("failed to add virtual channel resolve: %w", err)
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	available, err := s.svc.GetVirtualChannelResolveAmount(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to get virtual channel resolve: %w", err)
	}

	ps := s.activeChannels[string(key)]
	if ps == nil {
		channels, err := s.svc.GetChannelsWithNode(ctx, s.nodeKey)
		if err != nil {
			return fmt.Errorf("failed to get channels wth node: %w", err)
		}

		for _, channel := range channels {
			_, vc, err := channel.Their.State.FindVirtualChannel(key)
			if err != nil {
				if errors.Is(err, payments.ErrNotFound) {
					continue
				}
				return fmt.Errorf("failed to find virtual channel in %s: %w", channel.Address, err)
			}

			ps = &PaymentState{
				Key:      key,
				Deadline: time.Unix(vc.Deadline, 0),
				Capacity: vc.Capacity,
				Used:     big.NewInt(0),
				LastUsed: time.Now(),
			}
			s.activeChannels[string(key)] = ps
		}

		if ps == nil {
			return fmt.Errorf("virtual channel s not found")
		}
	}

	left := new(big.Int).Sub(available.Nano(), ps.Used)
	if left.Cmp(amount) == -1 {
		return fmt.Errorf("not enough available amount")
	}
	ps.Used = ps.Used.Add(ps.Used, amount)
	ps.LastUsed = time.Now()

	return nil
}

func (s *PaymentEngine) OpenVirtualChannel(ctx context.Context, capacity *big.Int, key ed25519.PublicKey) (ed25519.PrivateKey, *big.Int, error) {
	var tunChain = []transport.TunnelChainPart{
		{
			Target:   s.nodeKey,
			Capacity: capacity,
			Fee:      s.nodeFee,
			Deadline: time.Now().Add((30 + 30) * time.Minute),
		},
		{
			Target:   key,
			Capacity: capacity,
			Fee:      big.NewInt(0),
			Deadline: time.Now().Add(30 * time.Minute),
		},
	}
	fee := new(big.Int).Set(s.nodeFee)

	vPub, vPriv, _ := ed25519.GenerateKey(nil)
	vc, firstInstructionKey, tun, err := transport.GenerateTunnel(vPub, tunChain, 3)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate tunnel: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	err = s.svc.OpenVirtualChannel(ctx, s.nodeKey, firstInstructionKey, vPriv, tun, vc)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open virtual channel with node: %w", err)
	}

	return vPriv, fee, nil
}

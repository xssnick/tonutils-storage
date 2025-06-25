package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-storage-provider/pkg/contract"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
	"github.com/xssnick/tonutils-storage/db"
	"log"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var Logger = log.Println

type NewProviderData struct {
	Address       *address.Address
	MaxSpan       uint32
	PricePerMBDay tlb.Coins
}

type ProviderContractData struct {
	Size      uint64
	Address   *address.Address
	Providers []contract.ProviderDataV1
	Balance   tlb.Coins
}

type ProviderRates struct {
	Available        bool
	RatePerMBDay     tlb.Coins
	MinBounty        tlb.Coins
	SpaceAvailableMB uint64
	MinSpan          uint32
	MaxSpan          uint32

	Size uint64
}

type ProviderStorageInfo struct {
	StorageADNL string
	Status      string
	Reason      string
	Progress    float64

	Context   context.Context
	FetchedAt time.Time
}

type Client struct {
	storage  *db.Storage
	api      ton.APIClientWrapped
	provider *transport.Client

	adnlInfo  map[string][]byte
	infoCache map[string]*ProviderStorageInfo
	mx        sync.RWMutex
}

func NewClient(storage *db.Storage, api ton.APIClientWrapped, provider *transport.Client) *Client {
	return &Client{
		storage:   storage,
		api:       api,
		provider:  provider,
		adnlInfo:  map[string][]byte{},
		infoCache: map[string]*ProviderStorageInfo{},
	}
}

func (c *Client) FetchProviderContract(ctx context.Context, torrentHash []byte, owner *address.Address) (*ProviderContractData, error) {
	t := c.storage.GetTorrent(torrentHash)
	if t == nil {
		return nil, fmt.Errorf("torrent is not found")
	}
	if t.Info == nil {
		return nil, fmt.Errorf("info is not downloaded")
	}

	addr, _, _, err := contract.PrepareV1DeployData(torrentHash, t.Info.RootHash, t.Info.FileSize, t.Info.PieceSize, owner, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to calc contract addr: %w", err)
	}

	master, err := c.api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch master block: %w", err)
	}

	list, balance, err := contract.GetProvidersV1(ctx, c.api, master, addr)
	if err != nil {
		if errors.Is(err, contract.ErrNotDeployed) {
			return nil, contract.ErrNotDeployed
		}
		return nil, fmt.Errorf("failed to fetch providers list: %w", err)
	}

	return &ProviderContractData{
		Size:      t.Info.FileSize,
		Address:   addr,
		Providers: list,
		Balance:   balance,
	}, nil
}

func (c *Client) BuildAddProviderTransaction(ctx context.Context, torrentHash []byte, owner *address.Address, providers []NewProviderData) (addr *address.Address, bodyData, stateInit []byte, err error) {
	t := c.storage.GetTorrent(torrentHash)
	if t == nil {
		return nil, nil, nil, fmt.Errorf("torrent is not found")
	}
	if t.Info == nil {
		return nil, nil, nil, fmt.Errorf("info is not downloaded")
	}

	var prs []contract.ProviderV1
	for _, p := range providers {
		prs = append(prs, contract.ProviderV1{
			Address:       p.Address,
			MaxSpan:       p.MaxSpan,
			PricePerMBDay: p.PricePerMBDay,
		})
	}

	addr, si, body, err := contract.PrepareV1DeployData(torrentHash, t.Info.RootHash, t.Info.FileSize, t.Info.PieceSize, owner, prs)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to prepare contract data: %w", err)
	}

	siCell, err := tlb.ToCell(si)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("serialize state init: %w", err)
	}
	return addr, body.ToBOC(), siCell.ToBOC(), nil
}

func (c *Client) BuildWithdrawalTransaction(torrentHash []byte, owner *address.Address) (addr *address.Address, bodyData []byte, err error) {
	t := c.storage.GetTorrent(torrentHash)
	if t == nil {
		return nil, nil, fmt.Errorf("torrent is not found")
	}
	if t.Info == nil {
		return nil, nil, fmt.Errorf("info is not downloaded")
	}

	addr, body, err := contract.PrepareWithdrawalRequest(torrentHash, t.Info.RootHash, t.Info.FileSize, t.Info.PieceSize, owner)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare contract data: %w", err)
	}

	return addr, body.ToBOC(), nil
}

func (c *Client) FetchProviderRates(ctx context.Context, torrentHash, providerKey []byte) (*ProviderRates, error) {
	t := c.storage.GetTorrent(torrentHash)
	if t == nil {
		return nil, fmt.Errorf("torrent is not found")
	}
	if t.Info == nil {
		return nil, fmt.Errorf("info is not downloaded")
	}

	rates, err := c.provider.GetStorageRates(ctx, providerKey, t.Info.FileSize)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "value is not found"):
			return nil, errors.New("provider is not found")
		case strings.Contains(err.Error(), "context deadline exceeded"):
			return nil, errors.New("provider is not respond in a given time")
		}
		return nil, fmt.Errorf("failed to get rates: %w", err)
	}

	return &ProviderRates{
		Available:        rates.Available,
		RatePerMBDay:     tlb.FromNanoTON(new(big.Int).SetBytes(rates.RatePerMBDay)),
		MinBounty:        tlb.FromNanoTON(new(big.Int).SetBytes(rates.MinBounty)),
		SpaceAvailableMB: rates.SpaceAvailableMB,
		MinSpan:          rates.MinSpan,
		MaxSpan:          rates.MaxSpan,
		Size:             t.Info.FileSize,
	}, nil
}

func (c *Client) RequestProviderStorageInfo(ctx context.Context, torrentHash, providerKey []byte, owner *address.Address) (*ProviderStorageInfo, error) {
	t := c.storage.GetTorrent(torrentHash)
	if t == nil {
		return nil, fmt.Errorf("torrent is not found")
	}
	if t.Info == nil {
		return nil, fmt.Errorf("info is not downloaded")
	}

	addr, _, _, err := contract.PrepareV1DeployData(torrentHash, t.Info.RootHash, t.Info.FileSize, t.Info.PieceSize, owner, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to calc contract addr: %w", err)
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	mKey := addr.String() + "_" + hex.EncodeToString(providerKey)

	var tm time.Time
	v := c.infoCache[mKey]
	if v != nil {
		tm = v.FetchedAt
	} else {
		v = &ProviderStorageInfo{
			Status: "connecting...",
		}
		c.infoCache[mKey] = v
	}

	// run job if result is older than 10 sec and no another active job
	if time.Since(tm) > 5*time.Second && (v.Context == nil || v.Context.Err() != nil) {
		var end func()
		v.Context, end = context.WithTimeout(context.Background(), 60*time.Second)

		go func() {
			defer end()

			sn := time.Now()
			var storageADNL string
			proofByte := uint64(rand.Int63()) % t.Info.FileSize
			info, err := c.provider.RequestStorageInfo(v.Context, providerKey, addr, proofByte)
			if err != nil {
				Logger("failed to get storage info:", err, "took", time.Since(sn).String())

				c.mx.Lock()
				c.infoCache[mKey] = &ProviderStorageInfo{
					Status:    "inactive",
					Reason:    err.Error(),
					FetchedAt: time.Now(),
				}
				c.mx.Unlock()
				return
			}

			progress, _ := new(big.Float).Quo(new(big.Float).SetUint64(info.Downloaded), new(big.Float).SetUint64(t.Info.FileSize)).Float64()

			if info.Status == "active" {
				proved := false
				// verify proof
				proof, err := cell.FromBOC(info.Proof)
				if err == nil {
					if proofData, err := cell.UnwrapProof(proof, t.Info.RootHash); err == nil {
						piece := uint32(proofByte / uint64(t.Info.PieceSize))
						pieces := uint32(t.Info.FileSize / uint64(t.Info.PieceSize))

						if err = checkProofBranch(proofData, piece, pieces); err == nil {
							info.Reason = fmt.Sprintf("Storage proof received just now, but not connected with peer")
							proved = true
						}
					} else {
						Logger("failed to unwrap proof:", err)
					}
				}

				if !proved {
					info.Status = "untrusted"
					info.Reason = "Incorrect proof received"
				}
			} else if info.Status == "downloading" {
				info.Reason = fmt.Sprintf("Progress: %.2f", progress*100) + "%"
			} else if info.Status == "resolving" {
				info.Reason = fmt.Sprintf("Provider is trying to find source to download bag")
			} else if info.Status == "warning-balance" {
				info.Reason = fmt.Sprintf("Not enough balance to store bag, please topup or it will be deleted soon")
			}

			c.mx.RLock()
			adnlAddr := c.adnlInfo[mKey]
			c.mx.RUnlock()

			if adnlAddr == nil {
				xCtx, cancel := context.WithTimeout(v.Context, 10*time.Second)
				adnlAddr, err = c.provider.VerifyStorageADNLProof(xCtx, providerKey, addr)
				cancel()
				if err == nil {
					c.mx.Lock()
					c.adnlInfo[mKey] = adnlAddr
					c.mx.Unlock()
				}
			}

			if adnlAddr != nil {
				storageADNL = strings.ToUpper(hex.EncodeToString(adnlAddr))
			}

			c.mx.Lock()
			c.infoCache[mKey] = &ProviderStorageInfo{
				StorageADNL: storageADNL,
				Status:      info.Status,
				Reason:      info.Reason,
				Progress:    progress * 100,
				FetchedAt:   time.Now(),
			}
			c.mx.Unlock()
		}()
	}

	return v, nil
}

func checkProofBranch(proof *cell.Cell, piece, piecesNum uint32) error {
	if piece >= piecesNum {
		return fmt.Errorf("piece is out of range %d/%d", piece, piecesNum)
	}

	tree := proof.BeginParse()

	// calc tree depth
	depth := int(math.Log2(float64(piecesNum)))
	if piecesNum > uint32(math.Pow(2, float64(depth))) {
		// add 1 if pieces num is not exact log2
		depth++
	}

	// check bits from left to right and load branches
	for i := depth - 1; i >= 0; i-- {
		isLeft := piece&(1<<i) == 0

		b, err := tree.LoadRef()
		if err != nil {
			return err
		}

		if isLeft {
			tree = b
			continue
		}

		// we need right branch
		tree, err = tree.LoadRef()
		if err != nil {
			return err
		}
	}

	if tree.BitsLeft() != 256 {
		return fmt.Errorf("incorrect branch")
	}
	return nil
}

type Offer struct {
	Span          uint32
	Every         string
	RatePerMBNano *big.Int
	PerDayNano    *big.Int
	PerProofNano  *big.Int
}

func CalculateBestProviderOffer(r *ProviderRates) Offer {
	const minStep = 15 * 60

	if r.MinSpan < minStep {
		r.MinSpan = minStep
	}
	if r.MaxSpan < r.MinSpan {
		r.MaxSpan = r.MinSpan
	}

	step := (r.MaxSpan - r.MinSpan) / 300
	if step < minStep {
		step = minStep
	}

	var best Offer
	var bestCost *big.Int

	better := func(o Offer) bool {
		if bestCost == nil || o.PerDayNano.Cmp(bestCost) < 0 {
			return true
		}
		return o.PerDayNano.Cmp(bestCost) == 0 && o.Span < best.Span
	}

	for span := r.MinSpan; span <= r.MaxSpan; span += step {
		if o := calcOffer(span, r); better(o) {
			best, bestCost = o, o.PerDayNano
		}
	}

	if best.Span != r.MaxSpan {
		if o := calcOffer(r.MaxSpan, r); better(o) {
			best = o
		}
	}

	return best
}

func calcOffer(span uint32, r *ProviderRates) Offer {
	const mbMulDay = 86400 * 1024 * 1024

	size := new(big.Int).SetUint64(r.Size)
	spanBI := big.NewInt(int64(span))

	ratePerMBNano := new(big.Int).Set(r.RatePerMBDay.Nano())

	num := new(big.Int).Mul(r.MinBounty.Nano(), big.NewInt(mbMulDay))
	den := new(big.Int).Mul(size, spanBI)

	minRate := new(big.Int).Div(num, den)
	if new(big.Int).Mod(num, den).Sign() != 0 {
		minRate.Add(minRate, big.NewInt(1))
	}

	if ratePerMBNano.Cmp(minRate) < 0 {
		ratePerMBNano = minRate
	}

	bounty := new(big.Int).Mul(ratePerMBNano, size)
	bounty.Mul(bounty, spanBI).Div(bounty, big.NewInt(mbMulDay))

	proofsPerDay := new(big.Float).Quo(
		big.NewFloat(86400),
		new(big.Float).SetUint64(uint64(span)),
	)

	perProof := new(big.Float).SetInt(bounty)
	effPerDay := new(big.Float).Mul(perProof, proofsPerDay)

	perProofNano, _ := perProof.Int(nil)
	effPerDayNano, _ := effPerDay.Int(nil)

	var every string
	switch {
	case span < 3600:
		every = fmt.Sprintf("%d Minutes", span/60)
	case span < 100*3600:
		every = fmt.Sprintf("%d Hours", span/3600)
	default:
		every = fmt.Sprintf("%d Days", span/86400)
	}

	return Offer{
		Span:          span,
		Every:         every,
		RatePerMBNano: ratePerMBNano,
		PerDayNano:    effPerDayNano,
		PerProofNano:  perProofNano,
	}
}

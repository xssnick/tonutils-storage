package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/kevinms/leakybucket-go"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var Logger = func(...any) {}

type DHT interface {
	StoreAddress(ctx context.Context, addresses address.List, ttl time.Duration, ownerKey ed25519.PrivateKey, copies int) (int, []byte, error)
	FindAddresses(ctx context.Context, key []byte) (*address.List, ed25519.PublicKey, error)
	FindOverlayNodes(ctx context.Context, overlayId []byte, continuation ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error)
	Close()
}

type FileInfo struct {
	Size            uint64
	FromPiece       uint32
	ToPiece         uint32
	FromPieceOffset uint32
	ToPieceOffset   uint32
	Index           uint32
	Name            string
}

type TorrentDownloader interface {
	Close()
	IsActive() bool
}

type torrentDownloader struct {
	globalCtx      context.Context
	downloadCancel func()
}

type storagePeer struct {
	torrent      *Torrent
	nodeAddr     string
	overlay      []byte
	overlayNode  *overlay.Node
	nodeId       []byte
	sessionId    int64
	sessionSeqno int64
	conn         *PeerConnection

	currentPing           int64
	lastPingAt            time.Time
	lastNeighboursAt      time.Time
	sessionInitialized    int32
	sessionInitAt         int64
	updateInitReceived    int32
	lastUpdatePiecesAt    int64
	lastUpdatePiecesSeqno int64

	lastSentPieces []byte
	hasPieces      map[uint32]bool
	piecesMx       sync.RWMutex
	prepareInfoMx  sync.Mutex

	fails  int32
	failAt int64

	closeOnce sync.Once

	sessionCtx  context.Context
	stopSession func()

	closerCtx context.Context
	stop      func()
}

type TorrentInfo struct {
	PieceSize   uint32   `tlb:"## 32"`
	FileSize    uint64   `tlb:"## 64"`
	RootHash    []byte   `tlb:"bits 256"`
	HeaderSize  uint64   `tlb:"## 64"`
	HeaderHash  []byte   `tlb:"bits 256"`
	Description tlb.Text `tlb:"."`
}

type SpeedLimit struct {
	bytesPerSec uint64
	bucket      unsafe.Pointer
}

type TorrentServer interface {
	ConnectToNode(ctx context.Context, t *Torrent, node *overlay.Node, addrs *address.List) error
	GetADNLPrivateKey() ed25519.PrivateKey
	GetID() []byte
	Stop()
}

type Connector struct {
	downloadLimit *SpeedLimit
	uploadLimit   *SpeedLimit
	TorrentServer
}

func NewConnector(srv TorrentServer) *Connector {
	return &Connector{
		TorrentServer: srv,
		downloadLimit: &SpeedLimit{},
		uploadLimit:   &SpeedLimit{},
	}
}

func (s *SpeedLimit) SetLimit(bytesPerSec uint64) {
	if bytesPerSec == 0 {
		atomic.StoreUint64(&s.bytesPerSec, 0)
		atomic.StorePointer(&s.bucket, unsafe.Pointer(nil))
		return
	}

	if bytesPerSec > math.MaxInt64/3 {
		bytesPerSec = math.MaxInt64 / 3
	}

	atomic.StoreUint64(&s.bytesPerSec, bytesPerSec)

	b := leakybucket.NewLeakyBucket(float64(bytesPerSec), int64(bytesPerSec*3))
	atomic.StorePointer(&s.bucket, unsafe.Pointer(b))
}

func (s *SpeedLimit) GetLimit() uint64 {
	return atomic.LoadUint64(&s.bytesPerSec)
}

func (s *SpeedLimit) Throttle(_ context.Context, sz uint64) error {
	b := (*leakybucket.LeakyBucket)(atomic.LoadPointer(&s.bucket))
	if b != nil {
		full := uint64(b.Capacity())
		if sz < full {
			full = sz
		}

		if b.Remaining() < int64(full) || b.Add(int64(sz)) == 0 {
			return fmt.Errorf("limited")
		}
	}
	return nil
}

func (c *Connector) GetUploadLimit() uint64 {
	return c.uploadLimit.GetLimit()
}

func (c *Connector) GetDownloadLimit() uint64 {
	return c.downloadLimit.GetLimit()
}

func (c *Connector) SetDownloadLimit(bytesPerSec uint64) {
	c.downloadLimit.SetLimit(bytesPerSec)
}

func (c *Connector) SetUploadLimit(bytesPerSec uint64) {
	c.uploadLimit.SetLimit(bytesPerSec)
}

func (c *Connector) ThrottleDownload(ctx context.Context, sz uint64) error {
	return c.downloadLimit.Throttle(ctx, sz)
}

func (c *Connector) ThrottleUpload(ctx context.Context, sz uint64) error {
	return c.uploadLimit.Throttle(ctx, sz)
}

func (c *Connector) GetADNLPrivateKey() ed25519.PrivateKey {
	return c.TorrentServer.GetADNLPrivateKey()
}

func (c *Connector) CreateDownloader(ctx context.Context, t *Torrent) (_ TorrentDownloader, err error) {
	if len(t.BagID) != 32 {
		return nil, fmt.Errorf("invalid torrent bag id")
	}

	globalCtx, downloadCancel := context.WithCancel(ctx)
	var dow = &torrentDownloader{
		globalCtx:      globalCtx,
		downloadCancel: downloadCancel,
	}
	defer func() {
		if err != nil {
			downloadCancel()
		}
	}()

	// connect to first node and resolve torrent info
	for t.Info == nil {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("failed to find storage nodes for this bag, err: %w", ctx.Err())
			return nil, err
		case <-time.After(10 * time.Millisecond):
		}
	}

	if t.Header == nil {
		hdrPieces := uint32(t.Info.HeaderSize / uint64(t.Info.PieceSize))
		if t.Info.HeaderSize%uint64(t.Info.PieceSize) > 0 {
			// add not full piece
			hdrPieces++
		}

		hdrMask := make([]byte, hdrPieces)
		for i := range hdrPieces {
			hdrMask[i] = 1
		}

		pf := NewPreFetcher(globalCtx, t, nil, hdrPieces, hdrMask)
		defer pf.Stop()

		data := make([]byte, 0, uint64(hdrPieces)*uint64(t.Info.PieceSize))
		proofs := make([][]byte, 0, hdrPieces)
		for i := uint32(0); i < hdrPieces; i++ {
			piece, proof, pieceErr := pf.WaitGet(globalCtx, i)
			if pieceErr != nil {
				err = fmt.Errorf("failed to get header piece %d, err: %w", i, pieceErr)
				return nil, err
			}
			data = append(data, piece...)
			proofs = append(proofs, proof)
			pf.Free(i)
		}

		var header TorrentHeader
		data, err = tl.Parse(&header, data, true)
		if err != nil {
			err = fmt.Errorf("failed to load header from cell, err: %w", err)
			return nil, err
		}

		if len(header.DirName) > 256 {
			return nil, fmt.Errorf("too big dir name > 256")
		}

		if err := validateFileName(string(header.DirName), false); err != nil {
			return nil, fmt.Errorf("malicious bag: %w", err)
		}

		if header.FilesCount > 1_000_000 {
			return nil, fmt.Errorf("bag has > 1_000_000 files, looks dangerous")
		}
		if uint32(len(header.NameIndex)) != header.FilesCount ||
			uint32(len(header.DataIndex)) != header.FilesCount {
			err = fmt.Errorf("corrupted header, lack of files info")
			return nil, err
		}

		t.Header = &header

		for i, proof := range proofs {
			err = t.setPiece(uint32(i), &PieceInfo{
				StartFileIndex: 0,
				Proof:          proof,
			}, true)
			if err != nil {
				return nil, err
			}
		}
	}

	return dow, nil
}

func (p *storagePeer) Close() {
	p.closeOnce.Do(func() {
		Logger("[STORAGE] CLOSING CONNECTION OF", hex.EncodeToString(p.nodeId), p.nodeAddr, "BAG", hex.EncodeToString(p.torrent.BagID))
		p.stop()
		p.conn.CloseFor(p)
		p.torrent.RemovePeer(p.nodeId)
	})
}

func (p *storagePeer) initializeSession(ctx context.Context, id int64, doPing bool) bool {
	var err error
	defer func() {
		if err == nil {
			atomic.StoreInt32(&p.sessionInitialized, 1)
			p.torrent.wake.fire()

			Logger("[STORAGE] SESSION INITIALIZED FOR", hex.EncodeToString(p.nodeId), "BAG", hex.EncodeToString(p.torrent.BagID), "SESSION", atomic.LoadInt64(&p.sessionId))
			return
		}

		if atomic.LoadInt64(&p.sessionId) != id {
			return
		}

		Logger("[STORAGE] SESSION INITIALIZATION FAILED FOR", hex.EncodeToString(p.nodeId), "BAG", hex.EncodeToString(p.torrent.BagID), "SESSION", atomic.LoadInt64(&p.sessionId), "ERR", err.Error())
		p.Close()
	}()

	if doPing {
		qCtx, cancel := context.WithTimeout(ctx, 7*time.Second)
		err = p.ping(qCtx)
		cancel()
		if err != nil {
			err = fmt.Errorf("failed to ping: %w", err)
			return false
		}
	}

	if err = p.prepareTorrentInfo(ctx); err != nil {
		err = fmt.Errorf("failed to prepare torrent info, err: %w", err)
		return false
	}

	if err = p.updateInitPieces(ctx); err != nil {
		err = fmt.Errorf("failed to send init pieces, err: %w", err)
		return false
	}

	return true
}

func (p *storagePeer) touch() {
	p.torrent.TouchPeer(p)
}

func (p *storagePeer) findNeighbours(ctx context.Context) (*overlay.NodesList, error) {
	var al overlay.NodesList
	err := p.conn.adnl.Query(ctx, overlay.WrapQuery(p.overlay, &overlay.GetRandomPeers{}), &al)
	if err != nil {
		return nil, err
	}
	return &al, nil
}

func (p *storagePeer) ping(ctx context.Context) error {
	ses := atomic.LoadInt64(&p.sessionId)
	if ses == 0 {
		return fmt.Errorf("no session id")
	}

	tm := time.Now()
	var pong Pong
	err := p.conn.adnl.Query(ctx, overlay.WrapQuery(p.overlay, &Ping{SessionID: ses}), &pong)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&p.currentPing, int64(time.Since(tm)/time.Millisecond))

	return nil
}

func (p *storagePeer) downloadPiece(ctx context.Context, id uint32) (*Piece, int64, error) {
	tm := time.Now()

	var piece Piece
	err := func() error {
		err := p.conn.rldp.DoQuery(ctx, 4096+uint64(p.torrent.Info.PieceSize)*2, overlay.WrapQuery(p.overlay, &GetPiece{int32(id)}), &piece)
		if err != nil {
			return fmt.Errorf("failed to query piece %d. err: %w", id, err)
		}

		proof, err := cell.FromBOC(piece.Proof)
		if err != nil {
			return fmt.Errorf("failed to parse BoC of piece %d, err: %w", id, err)
		}

		err = cell.CheckProof(proof, p.torrent.Info.RootHash)
		if err != nil {
			return fmt.Errorf("proof check of piece %d failed: %w", id, err)
		}

		err = p.torrent.checkProofBranch(proof, piece.Data, id)
		if err != nil {
			return fmt.Errorf("proof branch check of piece %d failed: %w", id, err)
		}

		p.torrent.UpdateDownloadedPeer(p, uint64(len(piece.Data)))

		return nil
	}()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, time.Since(tm).Milliseconds(), err
		}

		now := time.Now().Unix()
		if old := atomic.LoadInt64(&p.failAt); old < now-1 && atomic.CompareAndSwapInt64(&p.failAt, old, now) {
			// in case 3 fails with 2s delay in a row, disconnect
			if atomic.AddInt32(&p.fails, 1) >= 3 {
				Logger("[STORAGE] TOO MANY FAILS FROM", p.nodeAddr, "CLOSING CONNECTION, ERR:", err.Error())
				// something wrong, close connection, we should reconnect after it
				p.Close()
			}
		}
		return nil, time.Since(tm).Milliseconds(), err
	}
	atomic.StoreInt32(&p.fails, 0)
	atomic.StoreInt64(&p.failAt, 0)

	return &piece, time.Since(tm).Milliseconds(), nil
}

func (t *Torrent) checkProofBranch(proof *cell.Cell, data []byte, piece uint32) error {
	piecesNum := t.Info.PiecesNum()
	if piece >= piecesNum {
		return fmt.Errorf("piece is out of range %d/%d", piece, piecesNum)
	}

	tree, err := proof.PeekRef(0)
	if err != nil {
		return err
	}

	// calc tree depth
	depth := int(math.Log2(float64(piecesNum)))
	if piecesNum > uint32(math.Pow(2, float64(depth))) {
		// add 1 if pieces num is not exact log2
		depth++
	}

	// check bits from left to right and load branches
	for i := depth - 1; i >= 0; i-- {
		refId := 1
		if piece&(1<<i) == 0 {
			refId = 0
		}

		tree, err = tree.PeekRef(refId)
		if err != nil {
			return err
		}
	}

	branchHash := tree.ToRawUnsafe().Data
	if len(branchHash) != 32 {
		return fmt.Errorf("hash in not 32 bytes")
	}

	h := sha256.Sum256(data)
	if !bytes.Equal(branchHash, h[:]) {
		return fmt.Errorf("incorrect branch hash %s | %s", hex.EncodeToString(branchHash), hex.EncodeToString(h[:]))
	}
	return nil
}

func (t *torrentDownloader) Close() {
	t.downloadCancel()
}

func (t *torrentDownloader) IsActive() bool {
	select {
	case <-t.globalCtx.Done():
		return false
	default:
		return true
	}
}

package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
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
	DownloadPiece(ctx context.Context, pieceIndex uint32) (_ []byte, err error)
	DownloadPieceDetailed(ctx context.Context, pieceIndex uint32) (data []byte, proof []byte, peer []byte, peerAddr string, err error)
	SetDesiredMinNodesNum(num int)
	Close()
	IsActive() bool
}

type torrentDownloader struct {
	piecesNum uint32

	desiredMinPeersNum int
	threadsPerPeer     int
	attempts           int

	torrent *Torrent

	mx sync.RWMutex

	globalCtx      context.Context
	downloadCancel func()
}

type pieceResponse struct {
	index int32
	node  *storagePeer
	piece Piece
	err   error
}

type pieceRequest struct {
	index  int32
	ctx    context.Context
	result chan<- pieceResponse
}

type storagePeer struct {
	torrent      *Torrent
	nodeAddr     string
	overlay      []byte
	nodeId       []byte
	sessionId    int64
	sessionSeqno int64
	conn         *PeerConnection

	lastSentPieces []byte
	hasPieces      map[uint32]bool
	piecesMx       sync.RWMutex

	fails int32
	loops int32

	pieceQueue chan *pieceRequest

	activateOnce sync.Once
	closeOnce    sync.Once
	globalCtx    context.Context
	stop         func()
}

type TorrentInfo struct {
	PieceSize   uint32   `tlb:"## 32"`
	FileSize    uint64   `tlb:"## 64"`
	RootHash    []byte   `tlb:"bits 256"`
	HeaderSize  uint64   `tlb:"## 64"`
	HeaderHash  []byte   `tlb:"bits 256"`
	Description tlb.Text `tlb:"."`
}

type speedLimit struct {
	bytesPerSec uint64
	limitUsed   uint64
	lastUsedAt  time.Time
	mx          sync.Mutex
}

type TorrentServer interface {
	StartPeerSearcher(t *Torrent)
}

type Connector struct {
	downloadLimit *speedLimit
	uploadLimit   *speedLimit
	TorrentServer
}

func NewConnector(srv TorrentServer) *Connector {
	return &Connector{
		TorrentServer: srv,
		downloadLimit: &speedLimit{},
		uploadLimit:   &speedLimit{},
	}
}

func (s *speedLimit) SetLimit(bytesPerSec uint64) {
	atomic.StoreUint64(&s.limitUsed, 0)
	atomic.StoreUint64(&s.bytesPerSec, bytesPerSec)
}

func (s *speedLimit) GetLimit() uint64 {
	return atomic.LoadUint64(&s.bytesPerSec)
}

func (s *speedLimit) Throttle(ctx context.Context, sz uint64) error {
	if atomic.LoadUint64(&s.bytesPerSec) > 0 {
		s.mx.Lock()
		defer s.mx.Unlock()

		select {
		case <-ctx.Done():
			// skip if not needed anymore
			return ctx.Err()
		default:
		}

		used := atomic.LoadUint64(&s.limitUsed)
		limit := atomic.LoadUint64(&s.bytesPerSec)
		if limit > 0 && used > limit {
			wait := time.Duration(float64(used)/float64(limit)*float64(time.Second)) - time.Since(s.lastUsedAt)
			if wait > 0 {
				select {
				case <-ctx.Done():
					// skip if not needed anymore
					return ctx.Err()
				case <-time.After(wait):
				}
			}
			atomic.StoreUint64(&s.limitUsed, 0)
		}
		s.lastUsedAt = time.Now()
		atomic.AddUint64(&s.limitUsed, sz)
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

func (c *Connector) CreateDownloader(ctx context.Context, t *Torrent, desiredMinPeersNum, threadsPerPeer int) (_ TorrentDownloader, err error) {
	if len(t.BagID) != 32 {
		return nil, fmt.Errorf("invalid torrent bag id")
	}

	globalCtx, downloadCancel := context.WithCancel(ctx)
	var dow = &torrentDownloader{
		torrent:            t,
		globalCtx:          globalCtx,
		downloadCancel:     downloadCancel,
		desiredMinPeersNum: desiredMinPeersNum,
		threadsPerPeer:     threadsPerPeer,
	}
	defer func() {
		if err != nil {
			downloadCancel()
		}
	}()

	if dow.torrent.Info == nil {
		// connect to first node and resolve torrent info
		for {
			select {
			case <-ctx.Done():
				err = fmt.Errorf("failed to find storage nodes for this bag, err: %w", ctx.Err())
				return nil, err
			case <-time.After(10 * time.Millisecond):
			}

			if dow.torrent.Info != nil {
				// info resolved
				break
			}
		}
	}
	dow.piecesNum = dow.torrent.PiecesNum()

	if dow.torrent.Header == nil {
		hdrPieces := dow.torrent.Info.HeaderSize / uint64(dow.torrent.Info.PieceSize)
		if dow.torrent.Info.HeaderSize%uint64(dow.torrent.Info.PieceSize) > 0 {
			// add not full piece
			hdrPieces++
		}

		data := make([]byte, 0, hdrPieces*uint64(dow.torrent.Info.PieceSize))
		proofs := make([][]byte, 0, hdrPieces)
		for i := uint32(0); i < uint32(hdrPieces); i++ {
			piece, proof, _, _, pieceErr := dow.DownloadPieceDetailed(globalCtx, i)
			if pieceErr != nil {
				err = fmt.Errorf("failed to get header piece %d, err: %w", i, pieceErr)
				return nil, err
			}
			data = append(data, piece...)
			proofs = append(proofs, proof)
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

		dow.torrent.Header = &header
		dow.torrent.InitMask()

		for i, proof := range proofs {
			err = dow.torrent.setPiece(uint32(i), &PieceInfo{
				StartFileIndex: 0,
				Proof:          proof,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return dow, nil
}

func (s *storagePeer) Close() {
	s.torrent.RemovePeer(s.nodeId)
	s.closeOnce.Do(func() {
		Logger("[STORAGE] CLOSING CONNECTION OF", hex.EncodeToString(s.nodeId), s.nodeAddr)
		s.stop()
		s.conn.CloseFor(s)
	})
}

func (s *storagePeer) touch() {
	s.torrent.TouchPeer(s)
	s.activateOnce.Do(func() {
		if !s.torrent.IsCompleted() {
			for i := 0; i < 20; i++ {
				go s.loop()
			}
			go s.pieceNotifier()
		}
	})
}

func (s *storagePeer) pinger(srv *Server) {
	defer func() {
		s.Close()
	}()

	var lastPeersReq time.Time

	startedAt := time.Now()
	fails := 0
	for {
		wait := 250 * time.Millisecond
		if s.sessionId != 0 {
			wait = 10 * time.Second
			// session should be initialised
			var pong Pong
			ctx, cancel := context.WithTimeout(s.globalCtx, 7*time.Second)
			err := s.conn.rldp.DoQuery(ctx, 1<<25, overlay.WrapQuery(s.overlay, &Ping{SessionID: s.sessionId}), &pong)
			cancel()
			if err != nil {
				fails++
				if fails >= 3 {
					Logger("[STORAGE] NODE NOT RESPOND 3 PINGS IN A ROW, CLOSING CONNECTION WITH ", hex.EncodeToString(s.nodeId), s.nodeAddr, err.Error())
					return
				}
			} else {
				fails = 0
				s.touch()
			}
		} else {
			if time.Since(startedAt) > 30*time.Second {
				sesId := rand.Int63()
				atomic.StoreInt64(&s.sessionId, sesId)
				atomic.StoreInt64(&s.sessionSeqno, 0)
				Logger("[STORAGE] FORCE NEW SESSION WITH", hex.EncodeToString(s.nodeId), sesId)
			}
		}

		if fails == 0 && time.Since(lastPeersReq) > 20*time.Second {
			Logger("[STORAGE] REQUESTING NODES LIST OF PEER", hex.EncodeToString(s.nodeId), "FOR", hex.EncodeToString(s.torrent.BagID))
			var al overlay.NodesList
			ctx, cancel := context.WithTimeout(s.globalCtx, 7*time.Second)
			err := s.conn.adnl.Query(ctx, overlay.WrapQuery(s.overlay, &overlay.GetRandomPeers{}), &al)
			cancel()
			if err == nil {
				for _, n := range al.List {
					// add known nodes in case we will need them in future to scale
					srv.addTorrentNode(&n, s.torrent)
				}
			} else {
				Logger("[STORAGE] FAILED REQUEST NODES LIST OF PEER", hex.EncodeToString(s.nodeId),
					"FOR", hex.EncodeToString(s.torrent.BagID), "ERR:", err.Error())
			}
			lastPeersReq = time.Now()
		}

		select {
		case <-s.globalCtx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (s *storagePeer) pieceNotifier() {
	lastReported := 0
	reportFails := 0
	for {
		select {
		case <-s.globalCtx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}

		s.torrent.newPiecesCond.L.Lock()
		for lastReported == s.torrent.DownloadedPiecesNum() {
			s.torrent.newPiecesCond.Wait()

			select {
			case <-s.globalCtx.Done():
				s.torrent.newPiecesCond.L.Unlock()
				return
			case <-s.torrent.completedCtx.Done():
				if lastReported == s.torrent.DownloadedPiecesNum() {
					// download completed and all pieces reported
					s.torrent.newPiecesCond.L.Unlock()
					return
				}
			default:
			}
		}
		s.torrent.newPiecesCond.L.Unlock()

		Logger("[STORAGE] NOTIFYING HAVE PIECES FOR PEER:", hex.EncodeToString(s.nodeId))
		ctx, cancel := context.WithTimeout(s.globalCtx, 5*time.Second)
		err := s.updateHavePieces(ctx, s.torrent)
		cancel()
		if err != nil {
			reportFails++
			Logger("[STORAGE] NOTIFY HAVE PIECES ERR:", err.Error())

			if reportFails > 3 {
				Logger("[STORAGE] TOO MANY FAILS FROM", s.nodeAddr, "CLOSING CONNECTION, ERR:", err.Error())

				s.Close()
				return
			}
			continue
		}

		reportFails = 0
		lastReported = s.torrent.DownloadedPiecesNum()
	}
}

func (s *storagePeer) loop() {
	returnNoClose := false
	atomic.AddInt32(&s.loops, 1)
	defer func() {
		atomic.AddInt32(&s.loops, -1)
		if !returnNoClose {
			s.Close()
		}
	}()

	for {
		var req *pieceRequest
		select {
		case <-s.globalCtx.Done():
			return
		case <-s.torrent.completedCtx.Done():
			Logger("[STORAGE] DOWNLOAD COMPLETED, CLOSING LOOP", hex.EncodeToString(s.nodeId), s.nodeAddr)
			returnNoClose = true
			return
		case req = <-s.pieceQueue:
			select {
			case <-req.ctx.Done():
				Logger("[STORAGE] ABANDONED PIECE TASK", req.index, "BY ", hex.EncodeToString(s.nodeId), s.nodeAddr)
				continue
			default:
			}

			Logger("[STORAGE] PICKED UP PIECE TASK", req.index, "BY ", hex.EncodeToString(s.nodeId), s.nodeAddr)
		}

		resp := pieceResponse{
			index: req.index,
			node:  s,
		}

		untrusted := false
		var piece Piece
		resp.err = func() error {
			reqCtx, cancel := context.WithTimeout(req.ctx, 7*time.Second)
			err := s.conn.rldp.DoQuery(reqCtx, 4096+int64(s.torrent.Info.PieceSize)*3, overlay.WrapQuery(s.overlay, &GetPiece{req.index}), &piece)
			cancel()
			if err != nil {
				return fmt.Errorf("failed to query piece %d. err: %w", req.index, err)
			}

			proof, err := cell.FromBOC(piece.Proof)
			if err != nil {
				untrusted = true
				return fmt.Errorf("failed to parse BoC of piece %d, err: %w", req.index, err)
			}

			err = cell.CheckProof(proof, s.torrent.Info.RootHash)
			if err != nil {
				untrusted = true
				return fmt.Errorf("proof check of piece %d failed: %w", req.index, err)
			}

			err = s.torrent.checkProofBranch(proof, piece.Data, uint32(req.index))
			if err != nil {
				untrusted = true
				return fmt.Errorf("proof branch check of piece %d failed: %w", req.index, err)
			}

			s.torrent.UpdateDownloadedPeer(s, uint64(len(piece.Data)))
			return nil
		}()
		if resp.err == nil {
			atomic.StoreInt32(&s.fails, 0)
			resp.piece = piece
		} else {
			Logger("[STORAGE] LOAD PIECE FROM", s.nodeAddr, "ERR:", resp.err.Error())
			atomic.AddInt32(&s.fails, 1)
		}
		req.result <- resp

		if resp.err != nil {
			if atomic.LoadInt32(&s.fails) >= 3*atomic.LoadInt32(&s.loops) || untrusted {
				Logger("[STORAGE] TOO MANY FAILS FROM", s.nodeAddr, "CLOSING CONNECTION, ERR:", resp.err.Error())
				// something wrong, close connection, we should reconnect after it
				return
			}

			select {
			case <-s.globalCtx.Done():
				return
			case <-time.After(300 * time.Millisecond):
				// TODO: take down all loops
				// take loop down for some time, to allow other nodes to pickup piece
			}
		}
	}
}

// DownloadPieceDetailed - same as DownloadPiece, but also returns proof data
func (t *torrentDownloader) DownloadPieceDetailed(ctx context.Context, pieceIndex uint32) (piece []byte, proof []byte, peer []byte, peerAddr string, err error) {
	resp := make(chan pieceResponse, 1)
	req := pieceRequest{
		index:  int32(pieceIndex),
		ctx:    ctx,
		result: resp,
	}

	skip := map[string]*storagePeer{}
	for {
		peers := t.torrent.GetPeers()

		var nodes = make([]*storagePeer, 0, len(peers))
		for _, node := range peers {
			if skip[string(node.peer.nodeId)] != nil {
				continue
			}

			node.peer.piecesMx.RLock()
			hasPiece := node.peer.hasPieces[pieceIndex]
			node.peer.piecesMx.RUnlock()

			if hasPiece {
				nodes = append(nodes, node.peer)
			}
		}

		if len(nodes) == 0 {
			select {
			case <-ctx.Done():
				return nil, nil, nil, "", ctx.Err()
			case <-time.After(250 * time.Millisecond):
				skip = map[string]*storagePeer{}
				// no nodes, wait
			}
			continue
		}

		// wait for one of desired nodes to accept task
		cases := make([]reflect.SelectCase, len(nodes)+1)
		for i, n := range nodes {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(n.pieceQueue), Send: reflect.ValueOf(&req)}
		}
		cases[len(nodes)] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}

		chId, _, recvOk := reflect.Select(cases)
		if recvOk && ctx.Err() != nil {
			return nil, nil, nil, "", ctx.Err()
		}

		select {
		case <-ctx.Done():
			return nil, nil, nil, "", ctx.Err()
		case result := <-resp:
			if result.err != nil {
				skip[string(nodes[chId].nodeId)] = nodes[chId]
				// try next node
				continue
			}
			return result.piece.Data, result.piece.Proof, result.node.nodeId, result.node.nodeAddr, nil
		}
	}
}

// DownloadPiece - downloads piece from one of available nodes.
// Can be used concurrently to download from multiple nodes in the same time
func (t *torrentDownloader) DownloadPiece(ctx context.Context, pieceIndex uint32) (_ []byte, err error) {
	piece, _, _, _, err := t.DownloadPieceDetailed(ctx, pieceIndex)
	return piece, err
}

func (t *Torrent) checkProofBranch(proof *cell.Cell, data []byte, piece uint32) error {
	piecesNum := t.PiecesNum()
	if piece >= piecesNum {
		return fmt.Errorf("piece is out of range %d/%d", piece, piecesNum)
	}

	tree, err := proof.BeginParse().LoadRef()
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

	branchHash, err := tree.LoadSlice(256)
	if err != nil {
		return err
	}

	dataHash := sha256.New()
	dataHash.Write(data)
	if !bytes.Equal(branchHash, dataHash.Sum(nil)) {
		return fmt.Errorf("incorrect branch hash")
	}
	return nil
}

func (t *torrentDownloader) SetDesiredMinNodesNum(num int) {
	t.desiredMinPeersNum = num
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

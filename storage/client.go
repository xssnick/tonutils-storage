package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
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
	"math"
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

	torrent     *Torrent
	dht         DHT
	knownNodes  map[string]*overlay.Node
	activeNodes map[string]*storageNode
	gate        *adnl.Gateway

	mx sync.RWMutex

	pieceQueue chan *pieceRequest

	globalCtx      context.Context
	downloadCancel func()
}

type pieceResponse struct {
	index int32
	node  *storageNode
	piece Piece
	err   error
}

type pieceRequest struct {
	index  int32
	ctx    context.Context
	result chan<- pieceResponse
}

type storageNode struct {
	nodeAddr  string
	nodeId    []byte
	dow       *torrentDownloader
	sessionId int64
	rawAdnl   overlay.ADNL
	rldp      *overlay.RLDPOverlayWrapper
	hasPieces map[uint32]bool
	piecesMx  sync.RWMutex

	fails int32
	loops int32

	globalCtx context.Context
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

type Connector struct {
	downloadLimit *speedLimit
	uploadLimit   *speedLimit

	gate *adnl.Gateway
	dht  DHT
}

func NewConnector(gate *adnl.Gateway, dht DHT) *Connector {
	return &Connector{
		gate:          gate,
		dht:           dht,
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

func (c *Connector) CreateDownloader(ctx context.Context, t *Torrent, desiredMinPeersNum, threadsPerPeer int, attempts ...int) (_ TorrentDownloader, err error) {
	tries := 2
	if len(attempts) > 0 {
		tries = attempts[0]
	}

	if len(t.BagID) != 32 {
		return nil, fmt.Errorf("invalid torrent bag id")
	}

	globalCtx, downloadCancel := context.WithCancel(ctx)
	var dow = &torrentDownloader{
		dht:                c.dht,
		gate:               c.gate,
		torrent:            t,
		activeNodes:        map[string]*storageNode{},
		knownNodes:         map[string]*overlay.Node{},
		globalCtx:          globalCtx,
		downloadCancel:     downloadCancel,
		pieceQueue:         make(chan *pieceRequest, 50+desiredMinPeersNum*threadsPerPeer),
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
		err = dow.scale(globalCtx, 1, tries)
		if err != nil {
			err = fmt.Errorf("failed to find storage nodes for this bag, err: %w", err)
			return nil, err
		}

		if dow.torrent.Info.PieceSize == 0 || dow.torrent.Info.HeaderSize == 0 {
			err = fmt.Errorf("incorrect torrent info sizes")
			return nil, err
		}
		if dow.torrent.Info.HeaderSize > 20*1024*1024 {
			err = fmt.Errorf("too big header > 20 MB, looks dangerous")
			return nil, err
		}
		if dow.torrent.Info.PieceSize > 64*1024*1024 {
			err = fmt.Errorf("too big piece > 64 MB, looks dangerous")
			return nil, err
		}
	}
	dow.piecesNum = dow.torrent.PiecesNum()

	go dow.scaleController()

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

		err = dow.torrent.BuildCache(int(hdrPieces))
		if err != nil {
			err = fmt.Errorf("failed to prepare header, err: %w", err)
			return nil, err
		}
	}

	return dow, nil
}

func (s *storageNode) Close() {
	Logger("[STORAGE_NODE] CLOSING CONNECTION OF", hex.EncodeToString(s.nodeId), s.nodeAddr)

	s.dow.torrent.RemovePeer(s.nodeId)
	s.rawAdnl.Close()
}

var errNoPieceOnNode = errors.New("node doesnt have this piece")

func (s *storageNode) loop() {
	atomic.AddInt32(&s.loops, 1)
	defer func() {
		atomic.AddInt32(&s.loops, -1)
		s.Close()
	}()

	for {
		var req *pieceRequest
		select {
		case <-s.globalCtx.Done():
			return
		case req = <-s.dow.pieceQueue:
		}

		resp := pieceResponse{
			index: req.index,
			node:  s,
		}

		s.piecesMx.RLock()
		has := s.hasPieces[uint32(req.index)]
		s.piecesMx.RUnlock()

		if !has {
			/*resp.err = errNoPieceOnNode
			req.result <- resp
			// TODO: make it better
			// give some time to other node to pickup piece
			time.Sleep(10 * time.Millisecond)
			continue*/
		}

		var piece Piece
		resp.err = func() error {
			reqCtx, cancel := context.WithTimeout(req.ctx, 7*time.Second)
			err := s.rldp.DoQuery(reqCtx, 4096+int64(s.dow.torrent.Info.PieceSize)*3, &GetPiece{req.index}, &piece)
			cancel()
			if err != nil {
				return fmt.Errorf("failed to query piece %d. err: %w", req.index, err)
			}
			s.dow.torrent.UpdateDownloadedPeer(s.nodeId, s.nodeAddr, uint64(len(piece.Data)))

			proof, err := cell.FromBOC(piece.Proof)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				return fmt.Errorf("failed to parse BoC of piece %d, err: %w", req.index, err)
			}

			err = cell.CheckProof(proof, s.dow.torrent.Info.RootHash)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				return fmt.Errorf("proof check of piece %d failed: %w", req.index, err)
			}

			err = s.dow.checkProofBranch(proof, piece.Data, uint32(req.index))
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				return fmt.Errorf("proof branch check of piece %d failed: %w", req.index, err)
			}
			return nil
		}()
		if resp.err == nil {
			atomic.StoreInt32(&s.fails, 0)
			resp.piece = piece
		} else {
			Logger("[DOWNLOADER] LOAD PIECE FROM", s.rawAdnl.RemoteAddr(), "ERR:", resp.err.Error())
			atomic.AddInt32(&s.fails, 1)
		}
		req.result <- resp

		if resp.err != nil {
			if atomic.LoadInt32(&s.fails) >= 3*atomic.LoadInt32(&s.loops) {
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

func (t *torrentDownloader) connectToNode(ctx context.Context, adnlID []byte, node *overlay.Node, onDisconnect func()) (*storageNode, error) {
	var addrs *address.List
	var keyN ed25519.PublicKey
	var err error
	for {
		Logger("[SCALER] LOOKING FOR NODE ADDR OF", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.torrent.BagID))

		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		addrs, keyN, err = t.dht.FindAddresses(ctx, adnlID)
		if err != nil {
			select {
			case <-ctx.Done():
				cancel()
				return nil, fmt.Errorf("failed to find node address: %w", err)
			default:
				cancel()
				continue
			}
		}
		cancel()
		break
	}

	Logger("[SCALER] ADDR FOR NODE ", hex.EncodeToString(adnlID), "FOUND", addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.torrent.BagID))

	addr := addrs.Addresses[0].IP.String() + ":" + fmt.Sprint(addrs.Addresses[0].Port)

	ax, err := t.gate.RegisterClient(addr, keyN)
	if err != nil {
		return nil, fmt.Errorf("failed to connnect to node: %w", err)
	}
	extADNL := overlay.CreateExtendedADNL(ax)
	rl := overlay.CreateExtendedRLDP(rldp.NewClientV2(extADNL)).CreateOverlay(node.Overlay)

	stNode := &storageNode{
		nodeId:    adnlID,
		nodeAddr:  addr,
		rawAdnl:   ax,
		rldp:      rl,
		dow:       t,
		hasPieces: map[uint32]bool{},
	}

	var sessionReady = make(chan int64, 1)
	var ready bool
	var readyMx sync.Mutex
	rl.SetOnQuery(func(transferId []byte, query *rldp.Query) error {
		ctx, cancel := context.WithTimeout(t.globalCtx, 30*time.Second)
		defer cancel()

		switch q := query.Data.(type) {
		case Ping:
			err = rl.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transferId, &Pong{})
			if err != nil {
				return err
			}

			readyMx.Lock()
			if !ready {
				var status Ok
				err = rl.DoQuery(ctx, 1<<25, &AddUpdate{
					SessionID: q.SessionID,
					Seqno:     1,
					Update: UpdateInit{
						HavePieces:       nil,
						HavePiecesOffset: 0,
						State: State{
							WillUpload:   false,
							WantDownload: true,
						},
					},
				}, &status)
				if err == nil { // if err - we will try again on next ping
					ready = true
					sessionReady <- q.SessionID
				}
			}
			readyMx.Unlock()
		case AddUpdate:
			switch u := q.Update.(type) {
			case UpdateInit:
				stNode.piecesMx.Lock()
				off := uint32(u.HavePiecesOffset)
				for i := 0; i < len(u.HavePieces); i++ {
					for y := 0; y < 8; y++ {
						if u.HavePieces[i]&(1<<y) > 0 {
							stNode.hasPieces[off+uint32(i*8+y)] = true
						}
					}
				}
				stNode.piecesMx.Unlock()
			case UpdateHavePieces:
				stNode.piecesMx.Lock()
				for _, d := range u.PieceIDs {
					stNode.hasPieces[uint32(d)] = true
				}
				stNode.piecesMx.Unlock()
			}
			// do nothing with this info for now, just ok
			err = rl.SendAnswer(ctx, query.MaxAnswerSize, query.ID, transferId, &Ok{})
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected rldp query received by storage cliet: %s", reflect.ValueOf(q).String())
		}
		return nil
	})

	Logger("[SCALER] REQUESTING TORRENT INFO FROM FROM", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.torrent.BagID))

	var res TorrentInfoContainer
	for {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = rl.DoQuery(ctx, 1<<25, &GetTorrentInfo{}, &res)
		if err != nil {
			select {
			case <-ctx.Done():
				cancel()
				return nil, err
			default:
				cancel()
				continue
			}
		}
		cancel()
		break
	}

	cl, err := cell.FromBOC(res.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse torrent info boc: %w", err)
	}

	if !bytes.Equal(cl.Hash(), t.torrent.BagID) {
		return nil, fmt.Errorf("incorrect torrent info")
	}

	nodeCtx, cancel := context.WithCancel(t.globalCtx)
	stNode.globalCtx = nodeCtx
	rl.SetOnDisconnect(func() {
		cancel()
		onDisconnect()
	})

	t.mx.Lock()
	if t.torrent.Info == nil {
		var info TorrentInfo
		err = tlb.LoadFromCell(&info, cl.BeginParse())
		if err != nil {
			t.mx.Unlock()
			ax.Close()
			return nil, fmt.Errorf("invalid torrent info cell")
		}
		t.torrent.Info = &info
	}
	t.mx.Unlock()

	// query first piece to be sure node is ready for downloading
	for {
		Logger("[SCALER] TRY LOAD PIECE FROM", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.torrent.BagID))
		qCtx, cancelC := context.WithTimeout(ctx, 7*time.Second)
		var piece Piece
		err = rl.DoQuery(qCtx, 4096+int64(t.torrent.Info.PieceSize)*3, &GetPiece{0}, &piece)
		cancelC()
		if err != nil {
			select {
			case <-ctx.Done():
				ax.Close()
				return nil, fmt.Errorf("failed to query first peice, err: %w", err)
			case <-time.After(1 * time.Second):
			}
			continue
		}
		Logger("[SCALER] GOT PIECE FROM", hex.EncodeToString(adnlID), addrs.Addresses[0].IP.String(), "FOR", hex.EncodeToString(t.torrent.BagID), "CONNECTED!")

		break
	}

	select {
	case id := <-sessionReady:
		stNode.sessionId = id
		return stNode, nil
	case <-ctx.Done():
		// close connection and all related overlays
		ax.Close()
		return nil, ctx.Err()
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

	for {
		select {
		case <-ctx.Done():
			return nil, nil, nil, "", ctx.Err()
		case t.pieceQueue <- &req:
		}

		select {
		case <-ctx.Done():
			return nil, nil, nil, "", ctx.Err()
		case result := <-resp:
			if result.err != nil {
				continue
				// return nil, fmt.Errorf("failed to query piece %d after retries: %w", pieceIndex, err)
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

func (t *torrentDownloader) checkProofBranch(proof *cell.Cell, data []byte, piece uint32) error {
	if piece >= t.piecesNum {
		return fmt.Errorf("piece is out of range %d/%d", piece, t.piecesNum)
	}

	tree, err := proof.BeginParse().LoadRef()
	if err != nil {
		return err
	}

	// calc tree depth
	depth := int(math.Log2(float64(t.piecesNum)))
	if t.piecesNum > uint32(math.Pow(2, float64(depth))) {
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

// scale - add more nodes to pool, to increase load speed and capacity
func (t *torrentDownloader) scale(ctx context.Context, num, attempts int) error {
	if num == 0 {
		return nil
	}

	var nodesDhtCont *dht.Continuation

	connections := make(chan bool, num)

	tries, amp := 0, 0
	checkedNodes := map[string]bool{}
	for {
		toCheck := make([]*overlay.Node, 0, len(t.knownNodes))
		t.mx.RLock()
		for _, node := range t.knownNodes {
			adnlID, err := adnl.ToKeyID(node.ID)
			if err != nil {
				continue
			}
			id := hex.EncodeToString(adnlID)

			isActive := t.activeNodes[id] != nil

			// will not connect to already active node
			if isActive {
				continue
			}

			// will not try again to connect in this scale iteration
			if checkedNodes[id] {
				continue
			}
			checkedNodes[id] = true

			toCheck = append(toCheck, node)
		}
		t.mx.RUnlock()

		nodesLeftToCheck := int64(len(toCheck))
		scaleDone := make(chan bool, 1)
		for _, node := range toCheck {
			adnlID, _ := adnl.ToKeyID(node.ID)
			id := hex.EncodeToString(adnlID)

			t.mx.Lock()
			t.activeNodes[id] = nil // mark node in progress of connection
			t.mx.Unlock()

			go func(node *overlay.Node) {
				defer func() {
					if atomic.AddInt64(&nodesLeftToCheck, -1) == 0 {
						// all nodes checked
						close(scaleDone)
					}
				}()

				onFail := func() {
					t.mx.Lock()
					delete(t.activeNodes, id)
					t.mx.Unlock()
				}

				scaleCtx, stopScale := context.WithTimeout(t.globalCtx, 35*time.Second)
				defer stopScale()

				Logger("[SCALER] CONNECTING TO PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.torrent.BagID))
				stNode, err := t.connectToNode(scaleCtx, adnlID, node, onFail)
				if err != nil {
					onFail()
					return
				}

				Logger("[SCALER] REQUESTING NODES LIST OF PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.torrent.BagID))
				var al overlay.NodesList
				err = stNode.rldp.DoQuery(scaleCtx, 1<<25, &overlay.GetRandomPeers{}, &al)
				if err != nil {
					stNode.Close()
					return
				}

				for _, n := range al.List {
					nodeId, err := adnl.ToKeyID(n.ID)
					if err != nil {
						continue
					}
					// add known nodes in case we will need them in future to scale
					t.mx.Lock()
					t.knownNodes[hex.EncodeToString(nodeId)] = &n
					t.mx.Unlock()
				}

				for i := 0; i < t.threadsPerPeer; i++ {
					go stNode.loop()
				}

				t.mx.Lock()
				t.activeNodes[id] = stNode
				t.mx.Unlock()
				Logger("[SCALER] ADDED PEER", hex.EncodeToString(adnlID), "FOR", hex.EncodeToString(t.torrent.BagID))

				select {
				case connections <- true:
				default:
					// already connected to enough nodes, no need to report more
				}
			}(node)
		}

		if len(toCheck) > 0 {
			timer := time.After(60 * time.Second)
		waiter:
			for {
				select {
				case connected := <-connections:
					if connected {
						num--
					}

					if num <= 0 {
						// we scaled enough
						return nil
					}
				case <-scaleDone:
					break waiter
					// all connection attempts finished
				case <-timer:
					// timeout for connections, lets try to find more nodes
					break waiter
				}
			}
		}

		var err error
		var nodes *overlay.NodesList

		tries++
		if tries%3 == 0 {
			// try again from start
			nodesDhtCont = nil
			checkedNodes = map[string]bool{}
		}

		Logger("[SCALER] SEARCHING STORAGE NODES FOR", hex.EncodeToString(t.torrent.BagID))

		ctxFind, cancel := context.WithTimeout(ctx, time.Duration(15+amp*5)*time.Second)
		nodes, nodesDhtCont, err = t.dht.FindOverlayNodes(ctxFind, t.torrent.BagID, nodesDhtCont)
		cancel()
		if err != nil {
			if attempts != -1 {
				attempts--
				if attempts == 0 {
					return fmt.Errorf("no nodes found")
				}
			}

			select {
			case <-ctx.Done():
				Logger("[SCALER] DHT CONTEXT CANCEL", hex.EncodeToString(t.torrent.BagID))

				return ctx.Err()
			case <-time.After(1 * time.Second):
				if amp < 10 {
					amp++
				}
				Logger("[SCALER] DHT RETRY", hex.EncodeToString(t.torrent.BagID))
				continue
			}
		}

		for i := range nodes.List {
			id, err := adnl.ToKeyID(nodes.List[i].ID)
			if err != nil {
				continue
			}
			// add known nodes in case we will need them in future to scale
			if t.knownNodes[hex.EncodeToString(id)] == nil {
				Logger("[SCALER] ADD KNOWN NODE ", hex.EncodeToString(id), "for", hex.EncodeToString(t.torrent.BagID))
			}
			t.knownNodes[hex.EncodeToString(id)] = &nodes.List[i]
		}
	}
}

func (t *torrentDownloader) scaleController() {
	for {
		t.mx.RLock()
		peersNum := len(t.activeNodes)
		t.mx.RUnlock()

		if peersNum < t.desiredMinPeersNum {
			_ = t.scale(t.globalCtx, t.desiredMinPeersNum-peersNum, 3)
		}

		select {
		case <-t.globalCtx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
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

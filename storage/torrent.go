package storage

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/tl"
	"io"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type FileIndex struct {
	BlockFrom       uint32
	BlockTo         uint32
	BlockFromOffset uint32
	BlockToOffset   uint32
	Name            string

	mx sync.Mutex
}

type OpenMode int

const (
	OpenModeRead OpenMode = iota
	OpenModeWrite
)

type FSController interface {
	AcquireRead(path string, p []byte, off int64) (n int, err error)
	RemoveFile(path string) error
}

type FSFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}

type FS interface {
	Open(name string, mode OpenMode) (FSFile, error)
	Delete(name string) error
	Exists(name string) bool
	GetController() FSController
}

type PieceInfo struct {
	StartFileIndex uint32
	Proof          []byte
}

type Storage interface {
	GetFS() FS
	GetAll() []*Torrent
	GetTorrentByOverlay(overlay []byte) *Torrent
	SetTorrent(torrent *Torrent) error
	SetActiveFiles(bagId []byte, ids []uint32) error
	GetActiveFiles(bagId []byte) ([]uint32, error)
	GetPiece(bagId []byte, id uint32) (*PieceInfo, error)
	RemovePiece(bagId []byte, id uint32) error
	SetPiece(bagId []byte, id uint32, p *PieceInfo) error
	PiecesMask(bagId []byte, num uint32) []byte
	UpdateUploadStats(bagId []byte, val uint64) error
	VerifyOnStartup() bool
	GetForcedPieceSize() uint32
}

type NetConnector interface {
	GetID() []byte
	GetADNLPrivateKey() ed25519.PrivateKey
	SetDownloadLimit(bytesPerSec uint64)
	SetUploadLimit(bytesPerSec uint64)
	GetUploadLimit() uint64
	GetDownloadLimit() uint64
	ThrottleDownload(ctx context.Context, sz uint64) error
	ThrottleUpload(ctx context.Context, sz uint64) error
	CreateDownloader(ctx context.Context, t *Torrent) (_ TorrentDownloader, err error)
	ConnectToNode(ctx context.Context, t *Torrent, node *overlay.Node, addrList *address.List) error
	TorrentServer
}

type TorrentStats struct {
	Uploaded                  uint64
	StoredUploaded            uint64
	LastUploadStatsStoredAtMs int64
}

type KnownNode struct {
	Node       atomic.Pointer[overlay.Node]
	FailStreak atomic.Uint32
	InProgress atomic.Bool

	TriedAt time.Time
}

type Torrent struct {
	BagID          []byte
	Path           string
	Info           *TorrentInfo
	Header         *TorrentHeader
	CreatedAt      time.Time
	CreatedLocally bool

	activeFiles     []uint32
	activeFileSet   map[uint32]struct{}
	activeUpload    bool
	downloadAll     bool
	downloadOrdered bool
	stats           TorrentStats
	uploadStatsMx   sync.Mutex

	connector  NetConnector
	downloader TorrentDownloader

	knownNodes map[string]*KnownNode
	peers      map[string]*PeerInfo
	peersMx    sync.RWMutex

	db Storage

	// completedCtx context.Context
	globalCtx context.Context
	pause     func()
	// complete     func()

	filesIndex map[string]uint32

	pieceMask                []byte
	downloadedPieces         atomic.Uint32
	downloadedPiecesCountSet atomic.Bool
	lastVerified             time.Time
	isVerificationInProgress bool

	lastDHTStoreAt          int64
	lastDHTStoreCompletedAt int64
	lastDHTStoreSuccessAt   int64
	lastDHTStoreFailed      int32
	dhtStoreFailStreak      uint32

	searchesWithZeroPeersNum uint32

	signalNewPieces chan struct{}
	newPieces       []uint32
	newPiecesBase   uint64
	newPiecesMx     sync.Mutex

	mx     sync.RWMutex
	maskMx sync.RWMutex
	// Verification must finish before a replacement downloader can write files.
	dataMx         sync.Mutex
	verificationMx sync.Mutex

	currentDownloadFlag *bool
	stopDownload        func()

	wake *wakeSig

	opWg sync.WaitGroup
}

func (t *Torrent) InitMask() {
	if err := t.InitMaskWithError(); err != nil {
		Logger("failed to initialize pieces mask:", err)
	}
}

func (t *Torrent) InitMaskWithError() error {
	t.maskMx.Lock()
	defer t.maskMx.Unlock()
	if len(t.pieceMask) == 0 {
		if t.Info == nil || t.Info.PieceSize == 0 {
			return fmt.Errorf("torrent metadata is not loaded")
		}
		if store, ok := t.db.(interface {
			PiecesMaskWithError([]byte, uint32) ([]byte, error)
		}); ok {
			mask, err := store.PiecesMaskWithError(t.BagID, t.Info.PiecesNum())
			if err != nil {
				return err
			}
			t.pieceMask = mask
		} else {
			t.pieceMask = t.db.PiecesMask(t.BagID, t.Info.PiecesNum())
		}
		t.downloadedPieces.Store(countBitsetOnes(t.pieceMask))
		t.downloadedPiecesCountSet.Store(true)
	}
	return nil
}

func (t *Torrent) GetConnector() NetConnector {
	return t.connector
}

func NewTorrent(path string, db Storage, connector NetConnector) *Torrent {
	t := &Torrent{
		Path:            path,
		CreatedAt:       time.Now(),
		peers:           map[string]*PeerInfo{},
		knownNodes:      map[string]*KnownNode{},
		db:              db,
		connector:       connector,
		signalNewPieces: make(chan struct{}, 1),
		wake:            newWakeSig(),
	}

	// create as stopped
	t.globalCtx, t.pause = context.WithCancel(context.Background())
	// t.completedCtx, t.complete = context.WithCancel(context.Background())
	t.pause()

	return t
}

func (t *Torrent) Wait() {
	t.opWg.Wait()
}

func (t *Torrent) IsDownloadAll() bool {
	t.mx.RLock()
	defer t.mx.RUnlock()
	return t.downloadAll
}

func (t *Torrent) IsDownloadOrdered() bool {
	t.mx.RLock()
	defer t.mx.RUnlock()
	return t.downloadOrdered
}

func (t *Torrent) GetUploadStats() uint64 {
	return atomic.LoadUint64(&t.stats.Uploaded)
}

func (t *Torrent) SetUploadStats(val uint64) {
	t.uploadStatsMx.Lock()
	defer t.uploadStatsMx.Unlock()
	atomic.StoreUint64(&t.stats.Uploaded, val)
	atomic.StoreUint64(&t.stats.StoredUploaded, val)
	atomic.StoreInt64(&t.stats.LastUploadStatsStoredAtMs, time.Now().UnixMilli())
}

func (t *Torrent) IsActive() (activeDownload, activeUpload bool) {
	t.mx.RLock()
	defer t.mx.RUnlock()
	select {
	case <-t.globalCtx.Done():
		return false, false
	default:
		return true, t.activeUpload && t.Header != nil
	}
}

func (t *Torrent) GetLastVerifiedAt() (bool, time.Time) {
	t.mx.RLock()
	defer t.mx.RUnlock()
	return t.isVerificationInProgress, t.lastVerified
}

func (t *Torrent) IsActiveRaw() (activeDownload, activeUpload bool) {
	t.mx.RLock()
	defer t.mx.RUnlock()
	select {
	case <-t.globalCtx.Done():
		return false, false
	default:
		return true, t.activeUpload
	}
}

func (t *Torrent) Stop() {
	defer t.wake.fire()

	t.mx.Lock()
	t.activeUpload = false
	t.pause()
	t.mx.Unlock()
	t.flushUploadStats()
}

func (t *Torrent) Start(withUpload, downloadAll, downloadOrdered bool) (err error) {
	defer t.wake.fire()

	t.mx.Lock()
	defer t.mx.Unlock()

	t.activeUpload = withUpload
	if t.globalCtx.Err() == nil && t.downloadAll == downloadAll && t.downloadOrdered == downloadOrdered {
		return nil
	}

	t.downloadAll = downloadAll
	t.downloadOrdered = downloadOrdered

	if t.pause != nil {
		t.pause()
	}

	t.globalCtx, t.pause = context.WithCancel(context.Background())

	t.opWg.Add(1)
	go t.peersManager(t.globalCtx)

	if t.db.VerifyOnStartup() && t.lastVerified.Before(time.Now().Add(-30*time.Second)) {
		workerCtx := t.globalCtx
		t.isVerificationInProgress = true
		t.opWg.Add(1)
		go func() {
			defer t.opWg.Done()
			if _, verifyErr := t.Verify(workerCtx, true); verifyErr != nil && workerCtx.Err() == nil {
				Logger("Verification of", hex.EncodeToString(t.BagID), "failed:", verifyErr.Error())
			}
		}()
		return nil
	}

	if t.CreatedLocally {
		return nil
	}
	return t.startDownload(nil)
}

func (t *Torrent) peersManager(workerCtx context.Context) {
	defer t.opWg.Done()
	defer Logger("[STORAGE_PEERS] PEER MANAGER STOPPED", "BAG", hex.EncodeToString(t.BagID))

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	connInflight := make(chan bool, 3)

	var lastUpdatePiecesSeqno int64
	var lastIdleSweepAt time.Time
	for {
		select {
		case <-workerCtx.Done():
			t.peersMx.RLock()
			peers := make([]*storagePeer, 0, len(t.peers))
			for _, peer := range t.peers {
				peers = append(peers, peer.peer)
			}
			t.peersMx.RUnlock()

			for _, peer := range peers {
				peer.Close()
			}

			return
		case <-ticker.C:
			select {
			case <-t.signalNewPieces:
				lastUpdatePiecesSeqno++
			default:
			}
		}
		ticker.Reset(time.Second)

		t.peersMx.RLock()
		peers := make([]*storagePeer, 0, len(t.peers))
		known := make([]*KnownNode, 0, len(t.knownNodes))
		for _, peer := range t.peers {
			peers = append(peers, peer.peer)
		}
		for id, k := range t.knownNodes {
			if t.peers[id] != nil {
				continue
			}
			known = append(known, k)
		}
		t.peersMx.RUnlock()

		rand.Shuffle(len(known), func(i, j int) {
			known[i], known[j] = known[j], known[i]
		})

		sort.Slice(peers, func(i, j int) bool {
			return peers[i].lastUpdatePiecesSeqno < peers[j].lastUpdatePiecesSeqno
		})

		var updates = 0
		nowTime := time.Now()
		now := nowTime.UnixMilli()
		idleSweepDue := lastIdleSweepAt.IsZero() || nowTime.Sub(lastIdleSweepAt) >= peerIdleSweepInterval
		wg := sync.WaitGroup{}
		type pieceUpdateResult struct {
			peer  *storagePeer
			seqno int64
		}
		pieceUpdates := make(chan pieceUpdateResult, 3)
		for _, peer := range peers {
			if idleSweepDue && peer.isIdle(nowTime, peerIdleTimeout) {
				Logger("[STORAGE_PEERS] PEER IDLE TOO LONG, CLOSING CONNECTION", hex.EncodeToString(peer.nodeId), "BAG", hex.EncodeToString(t.BagID), "LAST_ACTIVITY", peer.lastActivity().Format(time.RFC3339))
				peer.Close()
				continue
			}

			if !peer.isLocalInitSent() {
				continue
			}

			if updates < 3 && peer.lastUpdatePiecesSeqno < lastUpdatePiecesSeqno && peer.lastUpdatePiecesAt+1000 < now {
				peer.lastUpdatePiecesAt = now
				updateSeqno := lastUpdatePiecesSeqno
				updates++ // limit per cycle to not overload network

				wg.Add(1)
				go func() {
					defer wg.Done()

					Logger("[STORAGE_PEERS] DOING UPDATE HAVE PIECES FOR PEER", hex.EncodeToString(peer.nodeId), "BAG", hex.EncodeToString(t.BagID))

					if err := peer.updateHavePieces(workerCtx); err != nil {
						if !errors.Is(err, ErrQueueIsBusy) && atomic.AddInt32(&peer.fails, 1) > 3 {
							Logger("[STORAGE_PEERS] UPDATE HAVE PIECES FAILED FOR PEER", hex.EncodeToString(peer.nodeId), "AND TOO MANY FAILS, CLOSING CONNECTION", "BAG", hex.EncodeToString(t.BagID))
							peer.Close()
							return
						}
					} else {
						atomic.StoreInt32(&peer.fails, 0)
						pieceUpdates <- pieceUpdateResult{peer: peer, seqno: updateSeqno}
					}
				}()
			}

			if time.Since(peer.lastPingAt) > 15*time.Second {
				peer.lastPingAt = time.Now()
				wg.Add(1)
				go func() {
					defer wg.Done()

					qCtx, cancel := context.WithTimeout(workerCtx, 5*time.Second)
					defer cancel()

					err := peer.ping(qCtx)
					if err != nil && atomic.AddInt32(&peer.fails, 1) > 3 {
						Logger("[STORAGE_PEERS] PING FAILED FOR PEER", hex.EncodeToString(peer.nodeId), "AND TOO MANY FAILS, CLOSING CONNECTION", "BAG", hex.EncodeToString(t.BagID))
						peer.Close()
					} else if err == nil {
						atomic.StoreInt32(&peer.fails, 0)
					}
				}()
			} else if time.Since(peer.lastNeighboursAt) > 30*time.Second {
				peer.lastNeighboursAt = time.Now()
				wg.Add(1)
				go func() {
					defer wg.Done()

					qCtx, cancel := context.WithTimeout(workerCtx, 5*time.Second)
					defer cancel()

					nodes, err := peer.findNeighbours(qCtx)
					if err != nil && atomic.AddInt32(&peer.fails, 1) > 3 {
						Logger("[STORAGE_PEERS] FIND NEIGHBOURS FAILED FOR PEER", hex.EncodeToString(peer.nodeId), "AND TOO MANY FAILS, CLOSING CONNECTION", "BAG", hex.EncodeToString(t.BagID))
						peer.Close()
					} else if err == nil {
						atomic.StoreInt32(&peer.fails, 0)
						for _, node := range nodes.List {
							t.addNode(node)
						}
					}
				}()
			}
		}
		if idleSweepDue {
			lastIdleSweepAt = nowTime
		}

	loop:
		for _, node := range known {
			stk := node.FailStreak.Load()
			if stk > 8 {
				stk = 8
			}

			secDelay := 30 * (1 << stk) // exp backoff
			if secDelay > 1800 {
				secDelay = 1800
			}

			if node.TriedAt.Add(time.Duration(secDelay)*time.Second).After(time.Now()) || node.InProgress.Load() {
				continue
			}

			select {
			case connInflight <- true:
				node.TriedAt = time.Now()
				node.InProgress.Store(true)
			default:
				break loop
			}

			go func() {
				defer func() {
					// free slot
					<-connInflight
					node.InProgress.Store(false)
				}()

				ctx, cancel := context.WithTimeout(workerCtx, 90*time.Second)
				err := t.connector.ConnectToNode(ctx, t, node.Node.Load(), nil)
				cancel()
				if err != nil {
					node.FailStreak.Add(1)
					Logger("[STORAGE_PEERS] CONNECT TO NODE FAILED", err.Error())
					return
				}
				node.FailStreak.Store(0)
			}()
		}

		wg.Wait()
		close(pieceUpdates)
		for result := range pieceUpdates {
			result.peer.lastUpdatePiecesSeqno = result.seqno
		}
	}
}

func (t *TorrentInfo) PiecesNum() uint32 {
	piecesNum := t.FileSize / uint64(t.PieceSize)
	if t.FileSize%uint64(t.PieceSize) != 0 {
		piecesNum++
	}
	return uint32(piecesNum)
}

func (t *Torrent) getPiece(id uint32) (*PieceInfo, error) {
	return t.db.GetPiece(t.BagID, id)
}

func (t *Torrent) removePiece(id uint32) error {
	if err := t.db.RemovePiece(t.BagID, id); err != nil {
		return err
	}

	i := id / 8
	y := id % 8

	t.maskMx.Lock()
	t.ensureDownloadedPiecesCountLocked()
	if t.pieceMask[i]&(1<<y) != 0 {
		t.pieceMask[i] &= ^(1 << y)
		t.downloadedPieces.Add(^uint32(0))
	}
	t.maskMx.Unlock()
	if err := t.cleanupCachedPiece(id); err != nil {
		return fmt.Errorf("failed to remove cached piece %d: %w", id, err)
	}
	return nil
}

func (t *Torrent) setPiece(id uint32, p *PieceInfo, onlyHeader bool) error {
	if onlyHeader && t.Info.HeaderSize < t.Info.FileSize && t.Info.HeaderSize%uint64(t.Info.PieceSize) != 0 {
		if id == uint32(t.Info.HeaderSize/uint64(t.Info.PieceSize)) {
			// we should set an edge piece only when data is downloaded
			return nil
		}
	}

	i := id / 8
	y := id % 8

	if err := t.db.SetPiece(t.BagID, id, p); err != nil {
		return err
	}

	t.maskMx.Lock()
	t.ensureDownloadedPiecesCountLocked()
	if t.pieceMask[i]&(1<<y) == 0 {
		t.pieceMask[i] |= 1 << y
		t.downloadedPieces.Add(1)
	}
	t.maskMx.Unlock()

	t.enqueueNewPiece(id)

	// notify peers about our new pieces
	select {
	case t.signalNewPieces <- struct{}{}:
	default:
	}

	return nil
}

func (t *Torrent) PiecesMask() []byte {
	t.maskMx.RLock()
	defer t.maskMx.RUnlock()

	return append([]byte{}, t.pieceMask...)
}

func (t *Torrent) currentNewPiecesCursor() uint64 {
	t.newPiecesMx.Lock()
	defer t.newPiecesMx.Unlock()

	return t.newPiecesBase + uint64(len(t.newPieces))
}

func (t *Torrent) enqueueNewPiece(id uint32) {
	t.newPiecesMx.Lock()
	t.newPieces = append(t.newPieces, id)
	needCompact := len(t.newPieces) >= 4096
	t.newPiecesMx.Unlock()

	if needCompact {
		t.compactNewPieces()
	}
}

func (t *Torrent) getNewPiecesSince(cursor uint64, limit int) ([]int32, uint64) {
	t.newPiecesMx.Lock()
	defer t.newPiecesMx.Unlock()

	if cursor < t.newPiecesBase {
		cursor = t.newPiecesBase
	}

	offset := int(cursor - t.newPiecesBase)
	if offset >= len(t.newPieces) {
		return nil, cursor
	}

	end := len(t.newPieces)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	res := make([]int32, end-offset)
	for i, piece := range t.newPieces[offset:end] {
		res[i] = int32(piece)
	}

	return res, t.newPiecesBase + uint64(end)
}

func (t *Torrent) compactNewPieces() {
	t.peersMx.RLock()
	minCursor := uint64(0)
	hasReadyPeer := false
	for _, info := range t.peers {
		peer := info.peer
		if peer == nil {
			continue
		}

		peer.piecesMx.RLock()
		hasInitSnapshot := peer.lastSentPieces != nil
		peer.piecesMx.RUnlock()
		if !peer.isSessionReady() && !hasInitSnapshot {
			continue
		}

		cursor := atomic.LoadUint64(&peer.lastSentNewPiecesPos)
		if !hasReadyPeer || cursor < minCursor {
			minCursor = cursor
			hasReadyPeer = true
		}
	}
	t.peersMx.RUnlock()

	t.newPiecesMx.Lock()
	defer t.newPiecesMx.Unlock()

	tail := t.newPiecesBase + uint64(len(t.newPieces))
	if !hasReadyPeer {
		minCursor = tail
	}
	if minCursor < t.newPiecesBase {
		minCursor = t.newPiecesBase
	}
	if minCursor > tail {
		minCursor = tail
	}

	drop := int(minCursor - t.newPiecesBase)
	if drop <= 0 {
		return
	}

	if drop >= len(t.newPieces) {
		t.newPieces = nil
		t.newPiecesBase = minCursor
		return
	}

	t.newPieces = append([]uint32(nil), t.newPieces[drop:]...)
	t.newPiecesBase = minCursor
}

func (t *Torrent) IsCompleted() bool {
	if t.Info == nil {
		return false
	}

	num := t.Info.PiecesNum()
	return num > 0 && t.DownloadedPiecesNum() == int(num)
}

func (t *Torrent) DownloadedPiecesNum() int {
	t.maskMx.RLock()
	initialized := t.downloadedPiecesCountSet.Load()
	if initialized {
		pieces := t.downloadedPieces.Load()
		t.maskMx.RUnlock()
		return int(pieces)
	}
	mask := append([]byte(nil), t.pieceMask...)
	t.maskMx.RUnlock()

	return int(countBitsetOnes(mask))
}

func (t *Torrent) ensureDownloadedPiecesCountLocked() {
	if t.downloadedPiecesCountSet.Load() {
		return
	}
	t.downloadedPieces.Store(countBitsetOnes(t.pieceMask))
	t.downloadedPiecesCountSet.Store(true)
}

func (t *Torrent) LoadActiveFilesIDs() error {
	files, err := t.db.GetActiveFiles(t.BagID)
	if err != nil {
		return fmt.Errorf("failed to load active files from db: %w", err)
	}
	t.mx.Lock()
	t.activeFiles = files
	t.activeFileSet = fileIDsSet(files)
	t.mx.Unlock()
	return nil
}

func (t *Torrent) GetActiveFilesIDs() []uint32 {
	t.mx.RLock()
	defer t.mx.RUnlock()
	return append([]uint32(nil), t.activeFiles...)
}

func fileIDsSet(ids []uint32) map[uint32]struct{} {
	set := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func (t *Torrent) SetActiveFilesIDs(ids []uint32) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	if err := t.db.SetActiveFiles(t.BagID, ids); err != nil {
		return fmt.Errorf("failed to store active files in db: %w", err)
	}

	t.downloadAll = false
	t.activeFiles = append([]uint32(nil), ids...)
	t.activeFileSet = fileIDsSet(ids)
	return t.startDownload(nil)
}

func (t *Torrent) SetActiveFiles(names []string) error {
	if err := t.calcFileIndexes(); err != nil {
		return err
	}

	ids := make([]uint32, 0, len(names))
	for _, name := range names {
		val, ok := t.filesIndex[name]
		if !ok {
			return fmt.Errorf("file %s is not exist in torrent", name)
		}
		ids = append(ids, val)
	}
	return t.SetActiveFilesIDs(ids)
}

func (t *Torrent) GetPiece(id uint32) (*Piece, error) {
	t.mx.RLock()
	ctx := t.globalCtx
	t.mx.RUnlock()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("torrent paused")
	default:
	}
	return t.getPieceInternal(id, false)
}

func (t *Torrent) getPieceInternal(id uint32, verify bool) (*Piece, error) {
	t.mx.RLock()
	info, header := t.Info, t.Header
	downloadAll := t.downloadAll
	activeFileSet := t.activeFileSet
	t.mx.RUnlock()
	if info == nil || header == nil || info.PieceSize == 0 {
		return nil, fmt.Errorf("torrent metadata is not loaded")
	}
	if id >= info.PiecesNum() {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, info.PiecesNum())
	}
	piece, err := t.getPiece(id)
	if err != nil {
		return nil, fmt.Errorf("piece %d is not downloaded (%w)", id, err)
	}
	pieceStart := uint64(id) * uint64(info.PieceSize)
	pieceEnd := min(pieceStart+uint64(info.PieceSize), info.FileSize)
	var block []byte
	cached := piece.StartFileIndex == cachedPieceStartFileIndex
	if cached {
		var found bool
		block, found, err = t.readCachedPiece(id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("cache for piece %d is missing", id)
		}
		if !verify {
			return &Piece{Proof: piece.Proof, Data: block}, nil
		}
	} else {
		block = make([]byte, int(pieceEnd-pieceStart))
	}
	if pieceStart < info.HeaderSize {
		headerData, err := tl.Serialize(header, true)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize header: %w", err)
		}
		end := min(pieceEnd, info.HeaderSize)
		if end > uint64(len(headerData)) {
			return nil, fmt.Errorf("header size does not match torrent info")
		}
		copy(block, headerData[pieceStart:end])
	}
	metadata := &Torrent{Info: info, Header: header}
	files, err := metadata.GetFilesInPiece(id)
	if err != nil {
		return nil, err
	}
	if cached && !downloadAll && activeFileSet == nil {
		t.mx.RLock()
		activeFileSet = fileIDsSet(t.activeFiles)
		t.mx.RUnlock()
	}
	for _, file := range files {
		if cached && !downloadAll {
			if _, selected := activeFileSet[file.Index]; !selected {
				continue
			}
		}
		fileStart := uint64(file.FromPiece)*uint64(info.PieceSize) + uint64(file.FromPieceOffset)
		from := max(pieceStart, fileStart)
		to := min(pieceEnd, fileStart+file.Size)
		if to <= from {
			continue
		}
		filePath := filepath.Join(t.Path, string(header.DirName), file.Name)
		data := block[from-pieceStart : to-pieceStart]
		n, readErr := t.db.GetFS().GetController().AcquireRead(filePath, data, int64(from-fileStart))
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, fmt.Errorf("failed to read %s: %w", file.Name, readErr)
		}
		if n != len(data) {
			return nil, fmt.Errorf("short read of %s: %w", file.Name, io.ErrUnexpectedEOF)
		}
	}

	if verify {
		proof, err := cell.FromBOC(piece.Proof)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proof cell: %w", err)
		}

		if err = cell.CheckProof(proof, info.RootHash); err != nil {
			return nil, fmt.Errorf("proof check of piece %d failed: %w, %s", id, err, proof.Dump())
		}

		if err = t.checkProofBranch(proof, block, id); err != nil {
			return nil, fmt.Errorf("piece verification failed: %w", err)
		}
	}

	return &Piece{
		Proof: piece.Proof,
		Data:  block,
	}, nil
}

func (t *Torrent) GetPieceProof(id uint32) ([]byte, error) {
	if id >= t.Info.PiecesNum() {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, t.Info.PiecesNum())
	}

	piece, err := t.getPieceInternal(id, true)
	if err != nil {
		return nil, fmt.Errorf("piece %d error: %w", id, err)
	}

	return piece.Proof, nil
}

func (t *Torrent) SetInfoStats(pieceSize uint32, headerData, rootHash []byte, fileSize, headerSize uint64, description string) {
	t.Info = &TorrentInfo{
		PieceSize:  pieceSize,
		FileSize:   fileSize,
		RootHash:   rootHash,
		HeaderSize: headerSize,
		HeaderHash: calcHash(headerData),
		Description: tlb.Text{
			MaxFirstChunkSize: tlb.MaxTextChunkSize - 84, // 84 = size of prev data in bytes
			Value:             description,
		},
	}
}

func (t *Torrent) transmitTimeout() time.Duration {
	timeout := time.Duration(t.Info.PieceSize/(256<<10)) * time.Second
	if timeout < 15*time.Second {
		return 15 * time.Second
	} else if timeout > 60*time.Second {
		return 60 * time.Second
	}
	return timeout
}

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
	"math/bits"
	"path/filepath"
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
	Uploaded uint64
}

type Torrent struct {
	BagID          []byte
	Path           string
	Info           *TorrentInfo
	Header         *TorrentHeader
	CreatedAt      time.Time
	CreatedLocally bool

	activeFiles     []uint32
	activeUpload    bool
	downloadAll     bool
	downloadOrdered bool
	stats           TorrentStats

	connector  NetConnector
	downloader TorrentDownloader

	knownNodes map[string]*overlay.Node
	peers      map[string]*PeerInfo
	peersMx    sync.RWMutex

	db Storage

	// completedCtx context.Context
	globalCtx context.Context
	pause     func()
	// complete     func()

	filesIndex map[string]uint32

	pieceMask                []byte
	lastVerified             time.Time
	isVerificationInProgress bool

	lastDHTStore            time.Time
	lastDHTStoreCompletedAt int64
	lastDHTStoreFailed      int32

	searchesWithZeroPeersNum uint32

	signalNewPieces chan struct{}

	mx     sync.RWMutex
	maskMx sync.RWMutex

	currentDownloadFlag *bool
	stopDownload        func()

	opWg sync.WaitGroup
}

func (t *Torrent) InitMask() {
	t.maskMx.Lock()
	if len(t.pieceMask) == 0 {
		t.pieceMask = t.db.PiecesMask(t.BagID, t.Info.PiecesNum())
	}
	t.maskMx.Unlock()
}

func (t *Torrent) GetConnector() NetConnector {
	return t.connector
}

func NewTorrent(path string, db Storage, connector NetConnector) *Torrent {
	t := &Torrent{
		Path:            path,
		CreatedAt:       time.Now(),
		peers:           map[string]*PeerInfo{},
		knownNodes:      map[string]*overlay.Node{},
		db:              db,
		connector:       connector,
		signalNewPieces: make(chan struct{}, 1),
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
	return t.downloadAll
}

func (t *Torrent) IsDownloadOrdered() bool {
	return t.downloadOrdered
}

func (t *Torrent) GetUploadStats() uint64 {
	return atomic.LoadUint64(&t.stats.Uploaded)
}

func (t *Torrent) SetUploadStats(val uint64) {
	atomic.StoreUint64(&t.stats.Uploaded, val)
}

func (t *Torrent) IsActive() (activeDownload, activeUpload bool) {
	select {
	case <-t.globalCtx.Done():
		return false, false
	default:
		return true, t.activeUpload && t.Header != nil
	}
}

func (t *Torrent) GetLastVerifiedAt() (bool, time.Time) {
	return t.isVerificationInProgress, t.lastVerified
}

func (t *Torrent) IsActiveRaw() (activeDownload, activeUpload bool) {
	select {
	case <-t.globalCtx.Done():
		return false, false
	default:
		return true, t.activeUpload
	}
}

func (t *Torrent) Stop() {
	t.activeUpload = false
	t.pause()
}

func (t *Torrent) Start(withUpload, downloadAll, downloadOrdered bool) (err error) {
	t.activeUpload = withUpload

	t.mx.Lock()
	defer t.mx.Unlock()

	if d, _ := t.IsActive(); d && t.downloadAll == downloadAll && t.downloadOrdered == downloadOrdered {
		return nil
	}

	if !t.isVerificationInProgress && t.lastVerified.Before(time.Now().Add(-30*time.Second)) {
		if t.db.VerifyOnStartup() {
			t.isVerificationInProgress = true
			t.opWg.Add(1)
			go func() {
				defer t.opWg.Done()
				// it will remove corrupted pieces
				if _, err = t.Verify(t.globalCtx, true); err != nil {
					Logger("Verification of", hex.EncodeToString(t.BagID), "failed:", err.Error())
				}

				t.mx.Lock()
				defer t.mx.Unlock()

				t.lastVerified = time.Now()
				t.isVerificationInProgress = false
			}()
		}
	}

	t.downloadAll = downloadAll
	t.downloadOrdered = downloadOrdered

	if t.pause != nil {
		t.pause()
	}

	t.globalCtx, t.pause = context.WithCancel(context.Background())

	t.opWg.Add(1)
	go t.peersManager(t.globalCtx)

	if t.IsCompleted() {
		//	t.complete()
		return nil
	}

	currFlag := t.currentDownloadFlag
	currPause := t.pause
	return t.startDownload(func(event Event) {
		if event.Name == EventErr && currFlag == t.currentDownloadFlag {
			currPause()
		}
	})
}

func (t *Torrent) peersManager(workerCtx context.Context) {
	defer t.opWg.Done()
	defer Logger("[STORAGE_PEERS] PEER MANAGER STOPPED", "BAG", hex.EncodeToString(t.BagID))

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		var updatePieces bool
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
		case <-t.signalNewPieces:
			updatePieces = true
		case <-ticker.C:
		}
		ticker.Reset(time.Second)

		t.peersMx.RLock()
		peers := make([]*storagePeer, 0, len(t.peers))
		for _, peer := range t.peers {
			peers = append(peers, peer.peer)
		}
		t.peersMx.RUnlock()

		wg := sync.WaitGroup{}
		for _, peer := range peers {
			if atomic.LoadInt32(&peer.sessionInitialized) == 0 {
				continue
			}

			if updatePieces {
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
		wg.Wait()
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
	i := id / 8
	y := id % 8

	t.maskMx.Lock()
	t.pieceMask[i] &= ^(1 << y)
	t.maskMx.Unlock()

	return t.db.RemovePiece(t.BagID, id)
}

func (t *Torrent) setPiece(id uint32, p *PieceInfo) error {
	i := id / 8
	y := id % 8

	t.maskMx.Lock()
	t.pieceMask[i] |= 1 << y
	t.maskMx.Unlock()

	if err := t.db.SetPiece(t.BagID, id, p); err != nil {
		return err
	}

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

func (t *Torrent) IsCompleted() bool {
	mask := t.PiecesMask()
	if len(mask) == 0 {
		return false
	}

	num := t.Info.PiecesNum()
	for i, b := range mask {
		ones := 8
		if i == len(mask)-1 {
			if ones = int(num % 8); ones == 0 {
				ones = 8
			}
		}

		if bits.OnesCount8(b) != ones {
			return false
		}
	}
	return true
}

func (t *Torrent) DownloadedPiecesNum() int {
	mask := t.PiecesMask()

	pieces := 0
	for _, b := range mask {
		pieces += bits.OnesCount8(b)
	}
	return pieces
}

func (t *Torrent) LoadActiveFilesIDs() error {
	files, err := t.db.GetActiveFiles(t.BagID)
	if err != nil {
		return fmt.Errorf("failed to load active files from db: %w", err)
	}
	t.activeFiles = files
	return nil
}

func (t *Torrent) GetActiveFilesIDs() []uint32 {
	return t.activeFiles
}

func (t *Torrent) SetActiveFilesIDs(ids []uint32) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	if err := t.db.SetActiveFiles(t.BagID, ids); err != nil {
		return fmt.Errorf("failed to store active files in db: %w", err)
	}

	t.downloadAll = false
	t.activeFiles = ids
	currFlag := t.currentDownloadFlag
	currPause := t.pause
	return t.startDownload(func(event Event) {
		if event.Name == EventErr && currFlag == t.currentDownloadFlag {
			currPause()
		}
	})
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
	select {
	case <-t.globalCtx.Done():
		return nil, fmt.Errorf("torrent paused")
	default:
	}
	return t.getPieceInternal(id, false)
}

func (t *Torrent) getPieceInternal(id uint32, verify bool) (*Piece, error) {
	if id >= t.Info.PiecesNum() {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, t.Info.PiecesNum())
	}

	piece, err := t.getPiece(id)
	if err != nil {
		return nil, fmt.Errorf("piece %d is not downloaded (%w)", id, err)
	}

	offset := 0
	block := make([]byte, t.Info.PieceSize)

	var headerData []byte
	fileFrom := piece.StartFileIndex
	for {
		isHdr := t.Info.HeaderSize > uint64(id)*uint64(t.Info.PieceSize)+uint64(offset)

		// header
		if isHdr {
			if headerData == nil {
				headerData, err = tl.Serialize(t.Header, true)
				if err != nil {
					return nil, fmt.Errorf("failed to serialize header: %w", err)
				}
			}
			offset += copy(block[offset:], headerData[id*t.Info.PieceSize:])
		} else {
			f, err := t.GetFileOffsetsByID(fileFrom)
			if err != nil {
				return nil, fmt.Errorf("offsets for %d %d are not exists (%w)", id, fileFrom, err)
			}

			path := filepath.Join(t.Path, string(t.Header.DirName), f.Name)

			read := func(path string, from int64) error {
				n, err := t.db.GetFS().GetController().AcquireRead(path, block[offset:], from)
				if err != nil && err != io.EOF {
					return err
				}

				offset += n
				return nil
			}

			var fileOff int64 = 0
			if f.FromPiece != id {
				fileOff = int64(id-f.FromPiece)*int64(t.Info.PieceSize) - int64(f.FromPieceOffset)
			}

			if err = read(path, fileOff); err != nil {
				return nil, err
			}
			fileFrom++

			if fileFrom >= uint32(len(t.Header.DataIndex)) {
				// end reached
				break
			}
		}

		if offset == int(t.Info.PieceSize) {
			break
		}
	}

	if offset > 0 {
		block = block[:offset]
	}

	if verify {
		proof, err := cell.FromBOC(piece.Proof)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proof cell: %w", err)
		}

		if err = cell.CheckProof(proof, t.Info.RootHash); err != nil {
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
	if timeout < 7*time.Second {
		return 7 * time.Second
	} else if timeout > 60*time.Second {
		return 60 * time.Second
	}
	return timeout
}

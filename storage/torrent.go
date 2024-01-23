package storage

import (
	"context"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"io"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
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

type FSFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}

type FS interface {
	Open(name string, mode OpenMode) (FSFile, error)
	Exists(name string) bool
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
}

type NetConnector interface {
	SetDownloadLimit(bytesPerSec uint64)
	SetUploadLimit(bytesPerSec uint64)
	GetUploadLimit() uint64
	GetDownloadLimit() uint64
	ThrottleDownload(ctx context.Context, sz uint64) error
	ThrottleUpload(ctx context.Context, sz uint64) error
	CreateDownloader(ctx context.Context, t *Torrent, desiredMinPeersNum, threadsPerPeer int) (_ TorrentDownloader, err error)
	TorrentServer
}

type TorrentStats struct {
	Uploaded uint64
}

type Torrent struct {
	BagID     []byte
	Path      string
	Info      *TorrentInfo
	Header    *TorrentHeader
	CreatedAt time.Time

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

	memCache map[uint32]*Piece
	db       Storage

	completedCtx context.Context
	globalCtx    context.Context
	pause        func()
	complete     func()

	filesIndex map[string]uint32

	pieceMask []byte

	mx            sync.Mutex
	maskMx        sync.RWMutex
	newPiecesCond *sync.Cond

	currentDownloadFlag *bool
	stopDownload        func()
}

var fs = NewFSController()

func (t *Torrent) InitMask() {
	t.maskMx.Lock()
	t.pieceMask = t.db.PiecesMask(t.BagID, t.PiecesNum())
	t.maskMx.Unlock()
}

func (t *Torrent) GetConnector() NetConnector {
	return t.connector
}

func (t *Torrent) BuildCache(cachePiecesNum int) error {
	t.memCache = map[uint32]*Piece{}

	num := t.PiecesNum()
	if cachePiecesNum > int(num) {
		cachePiecesNum = int(num)
	}

	t.memCache = map[uint32]*Piece{}
	for i := 0; i < cachePiecesNum; i++ {
		p, err := t.getPieceInternal(uint32(i))
		if err != nil {
			return err
		}
		t.memCache[uint32(i)] = p
	}

	return nil
}

func NewTorrent(path string, db Storage, connector NetConnector) *Torrent {
	t := &Torrent{
		Path:          path,
		CreatedAt:     time.Now(),
		peers:         map[string]*PeerInfo{},
		memCache:      map[uint32]*Piece{},
		knownNodes:    map[string]*overlay.Node{},
		db:            db,
		connector:     connector,
		newPiecesCond: sync.NewCond(&sync.Mutex{}),
	}

	// create as stopped
	t.globalCtx, t.pause = context.WithCancel(context.Background())
	t.completedCtx, t.complete = context.WithCancel(context.Background())
	t.pause()

	return t
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

	t.downloadAll = downloadAll
	t.downloadOrdered = downloadOrdered

	if t.pause != nil {
		t.pause()
	}

	t.globalCtx, t.pause = context.WithCancel(context.Background())
	t.completedCtx, t.complete = context.WithCancel(context.Background())
	go t.runPeersMonitor()
	go t.connector.StartPeerSearcher(t)

	if t.IsCompleted() {
		t.complete()
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

func (t *Torrent) PiecesNum() uint32 {
	piecesNum := t.Info.FileSize / uint64(t.Info.PieceSize)
	if t.Info.FileSize%uint64(t.Info.PieceSize) != 0 {
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
	t.newPiecesCond.Broadcast()

	if t.IsCompleted() {
		t.complete()
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

	for i, b := range mask {
		ones := 8
		if i == len(mask)-1 {
			if ones = int(t.PiecesNum() % 8); ones == 0 {
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
	return t.getPieceInternal(id)
}

func (t *Torrent) getPieceInternal(id uint32) (*Piece, error) {
	if t.memCache != nil {
		p := t.memCache[id]
		if p != nil {
			return p, nil
		}
	}

	if id >= t.PiecesNum() {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, t.PiecesNum())
	}

	piece, err := t.getPiece(id)
	if err != nil {
		return nil, fmt.Errorf("piece %d is not downlaoded (%w)", id, err)
	}

	offset := 0
	block := make([]byte, t.Info.PieceSize)

	fileFrom := piece.StartFileIndex
	for {
		isHdr := t.Info.HeaderSize > uint64(id)*uint64(t.Info.PieceSize)+uint64(offset)

		// header
		if isHdr {
			headerData, err := tl.Serialize(t.Header, true)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize header: %w", err)
			}
			offset += copy(block[offset:], headerData[id*t.Info.PieceSize:])
		} else {
			f, err := t.GetFileOffsetsByID(fileFrom)
			if err != nil {
				return nil, fmt.Errorf("offsets for %d %d are not exists (%w)", id, fileFrom, err)
			}

			path := t.Path + "/" + string(t.Header.DirName) + "/" + f.Name
			read := func(path string, from int64) error {
				fd, err := fs.Acquire(path)
				if err != nil {
					return err
				}
				defer fd.Free()

				n, err := fd.Get().ReadAt(block[offset:], from)
				if err != nil && err != io.EOF {
					return err
				}

				offset += n
				return nil
			}

			fileOff := uint32(0)
			if f.FromPiece != id {
				fileOff = (id-f.FromPiece)*t.Info.PieceSize - f.FromPieceOffset
			}
			err = read(path, int64(fileOff))
			if err != nil {
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

	return &Piece{
		Proof: piece.Proof,
		Data:  block,
	}, nil
}

func (t *Torrent) GetPieceProof(id uint32) ([]byte, error) {
	if id >= t.PiecesNum() {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, t.PiecesNum())
	}

	piece, err := t.getPiece(id)
	if err != nil {
		return nil, fmt.Errorf("piece %d is not downlaoded (%w)", id, err)
	}

	return piece.Proof, nil
}

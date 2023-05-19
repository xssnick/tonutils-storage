package storage

import (
	"context"
	"fmt"
	"github.com/xssnick/tonutils-go/tl"
	"io"
	"sync"
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

type PieceInfo struct {
	StartFileIndex uint32
	Proof          []byte
}

type PeerInfo struct {
	LastSeenAt time.Time
	Uploaded   uint64
	Downloaded uint64
}

type Storage interface {
	GetFileIndex(bagId []byte, id uint32) (*FileIndex, error)
	SetFileIndex(bagId []byte, id uint32, fi *FileIndex) error
	GetPiece(bagId []byte, id uint32) (*PieceInfo, error)
	RemovePiece(bagId []byte, id uint32) error
	SetPiece(bagId []byte, id uint32, p *PieceInfo) error
	PiecesMask(bagId []byte, num uint32) []byte
}

type Torrent struct {
	BagID  []byte
	Path   string
	Info   *TorrentInfo
	Header *TorrentHeader

	peers   map[string]*PeerInfo
	peersMx sync.RWMutex

	memCache map[uint32]*Piece
	db       Storage

	globalCtx context.Context
	close     func()

	filesIndexCalc sync.Once
	filesIndex     map[string]uint32

	pieceMask []byte

	mx sync.Mutex
}

var fs = NewFSController()

func (t *Torrent) InitMask() {
	t.pieceMask = t.db.PiecesMask(t.BagID, t.piecesNum())
}

func (t *Torrent) BuildCache(cachePiecesNum int) error {
	t.memCache = map[uint32]*Piece{}

	num := t.piecesNum()
	if cachePiecesNum > int(num) {
		cachePiecesNum = int(num)
	}

	t.memCache = map[uint32]*Piece{}
	for i := 0; i < cachePiecesNum; i++ {
		p, err := t.GetPiece(uint32(i))
		if err != nil {
			return err
		}
		t.memCache[uint32(i)] = p
	}

	return nil
}

func NewTorrent(path string, db Storage, started bool) *Torrent {
	t := &Torrent{
		Path:     path,
		peers:    map[string]*PeerInfo{},
		memCache: map[uint32]*Piece{},
		db:       db,
	}
	t.Start()

	if !started {
		t.Pause()
	}

	return t
}

func (t *Torrent) IsActive() bool {
	select {
	case <-t.globalCtx.Done():
		return false
	default:
		return true
	}
}

func (t *Torrent) Pause() {
	t.close()
}

func (t *Torrent) Start() {
	t.globalCtx, t.close = context.WithCancel(context.Background())
	go t.runPeersMonitor()
}

func (t *Torrent) piecesNum() uint32 {
	piecesNum := t.Info.FileSize / uint64(t.Info.PieceSize)
	if t.Info.FileSize%uint64(t.Info.PieceSize) != 0 {
		piecesNum++
	}
	return uint32(piecesNum)
}

func (t *Torrent) getFileIndex(id uint32) (*FileIndex, error) {
	return t.db.GetFileIndex(t.BagID, id)
}

func (t *Torrent) setFileIndex(id uint32, fi *FileIndex) error {
	return t.db.SetFileIndex(t.BagID, id, fi)
}

func (t *Torrent) getPiece(id uint32) (*PieceInfo, error) {
	return t.db.GetPiece(t.BagID, id)
}

func (t *Torrent) removePiece(id uint32) error {
	i := id / 8
	y := id % 8
	t.pieceMask[i] &= 1 << y
	return t.db.RemovePiece(t.BagID, id)
}

func (t *Torrent) setPiece(id uint32, p *PieceInfo) error {
	i := id / 8
	y := id % 8
	t.pieceMask[i] |= 1 << y
	return t.db.SetPiece(t.BagID, id, p)
}

func (t *Torrent) PiecesMask() []byte {
	return t.pieceMask
}

func (t *Torrent) GetPiece(id uint32) (*Piece, error) {
	select {
	case <-t.globalCtx.Done():
		return nil, fmt.Errorf("torrent paused")
	default:
	}

	if t.memCache != nil {
		p := t.memCache[id]
		if p != nil {
			return p, nil
		}
	}

	if id >= t.piecesNum() {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, t.piecesNum())
	}

	piece, err := t.getPiece(id)
	if err != nil {
		return nil, fmt.Errorf("piece %d is not downlaoded (%w)", id, err)
	}

	offset := 0
	block := make([]byte, t.Info.PieceSize)

	fileFrom := piece.StartFileIndex
	for {
		f, err := t.getFileIndex(fileFrom)
		if err != nil {
			break
		}

		// header
		if f.Name == "" {
			headerData, err := tl.Serialize(&t.Header, true)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize header: %w", err)
			}
			offset += copy(block[offset:], headerData[id*t.Info.PieceSize:])
		} else {
			path := t.Path + "/" + f.Name
			read := func(path string, from int64) error {
				f.mx.Lock()
				defer f.mx.Unlock()

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
			if f.BlockFrom != id {
				fileOff = (id-f.BlockFrom)*t.Info.PieceSize - f.BlockFromOffset
			}
			err := read(path, int64(fileOff))
			if err != nil {
				return nil, err
			}
		}

		if offset == int(t.Info.PieceSize) {
			break
		}
		fileFrom++
	}

	if offset > 0 {
		block = block[:offset]
	}

	return &Piece{
		Proof: piece.Proof,
		Data:  block,
	}, nil
}

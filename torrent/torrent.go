package torrent

import (
	"crypto/sha256"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl/storage"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"io"
	"sync"
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
	HashForProof   []byte
	StartFileIndex int
}

type Torrent struct {
	BagID      []byte
	Path       string
	Info       storage.TorrentInfo
	Header     storage.TorrentHeader
	HeaderData []byte
	PieceIndex []*PieceInfo
	Index      []*FileIndex
	HashTree   *cell.Cell
	MemCache   map[uint32]*storage.Piece `json:"-"`
}

var fs = NewFSController()

func (t *Torrent) BuildCache() error {
	t.MemCache = map[uint32]*storage.Piece{}
	for i := 0; i < len(t.PieceIndex); i++ {
		p, err := t.GetPiece(uint32(i))
		if err != nil {
			return err
		}
		t.MemCache[uint32(i)] = p
	}
	return nil
}

func (t *Torrent) GetPiece(id uint32) (*storage.Piece, error) {
	//	tm := time.Now()
	//	defer func() {
	//		log.Println("Sent piece", id, "took", time.Since(tm).String())
	//	}()

	if t.MemCache != nil {
		p := t.MemCache[id]
		if p != nil {
			return p, nil
		}
	}

	if int(id) >= len(t.PieceIndex) {
		return nil, fmt.Errorf("piece %d not found, pieces count: %d", id, len(t.PieceIndex))
	}

	piece := t.PieceIndex[id]
	proof, err := t.HashTree.CreateProof([][]byte{piece.HashForProof})
	if err != nil {
		return nil, err
	}

	offset := 0
	block := make([]byte, t.Info.PieceSize)

	fileFrom := piece.StartFileIndex
	for {
		if fileFrom >= len(t.Index) {
			break
		}

		f := t.Index[fileFrom]
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
			err = read(path, int64(fileOff))
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

	dataHash := sha256.New()
	dataHash.Write(block)
	return &storage.Piece{
		Proof: proof.ToBOCWithFlags(false),
		Data:  block,
	}, nil
}

package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"io"
	"math"
	"path/filepath"
)

type fileInfoData struct {
	Path string
	Name string
}

type FileRef interface {
	GetName() string
	GetSize() uint64
	CreateReader() (io.ReadCloser, error)
}

func CreateTorrent(filesRootPath, dirName, description string, db Storage, connector NetConnector, files []FileRef) (*Torrent, error) {
	const pieceSize = 128 * 1024

	cb := make([]byte, pieceSize)
	cbOffset := 0

	torrent := NewTorrent(filepath.Dir(filesRootPath), db, connector)
	torrent.Header = &TorrentHeader{
		DirNameSize: uint32(len(dirName)),
		DirName:     []byte(dirName),
	}

	waiter, _ := pterm.DefaultSpinner.Start("Scanning files...")

	var dataSize uint64
	for _, file := range files {
		name := file.GetName()
		torrent.Header.FilesCount++
		torrent.Header.TotalNameSize += uint64(len(name))
		torrent.Header.Names = append(torrent.Header.Names, name...)
		torrent.Header.NameIndex = append(torrent.Header.NameIndex, torrent.Header.TotalNameSize)

		dataSize += file.GetSize()

		torrent.Header.DataIndex = append(torrent.Header.DataIndex, dataSize)
	}

	filesProcessed := uint32(0)

	hashes := make([][]byte, 0, 256)
	pieces := make([]*PieceInfo, 0, 256)

	pieceStartFileIndex := uint32(0)

	process := func(name string, isHeader bool, rd io.Reader) error {
		for {
			if cbOffset == 0 {
				pieceStartFileIndex = filesProcessed
			}

			n, err := rd.Read(cb[cbOffset:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			cbOffset += n

			if cbOffset == len(cb) {
				hash := calcHash(cb)
				hashes = append(hashes, hash)

				// save index of file where block starts
				pieces = append(pieces, &PieceInfo{
					StartFileIndex: pieceStartFileIndex,
				})

				cbOffset = 0
			}
		}

		if !isHeader { // if not header
			filesProcessed++
		}
		return nil
	}
	waiter.Success()

	waiter, _ = pterm.DefaultSpinner.Start("Generating bag header...")
	headerData, err := tl.Serialize(torrent.Header, true)
	if err != nil {
		waiter.Fail(err.Error())
		return nil, fmt.Errorf("failed to serialize header: %w", err)
	}
	err = process("", true, bytes.NewBuffer(headerData))
	if err != nil {
		waiter.Fail(err.Error())
		return nil, fmt.Errorf("failed to process header piece: %w", err)
	}
	waiter.Success()

	progress, _ := pterm.DefaultProgressbar.WithTotal(len(files)).WithTitle("Validating files...").Start()

	// add files
	for _, f := range files {
		rd, err := f.CreateReader()
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", f.GetName(), err)
		}

		err = process(f.GetName(), false, rd)
		_ = rd.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to process file %s: %w", f.GetName(), err)
		}
		progress.Increment()
	}

	waiter, _ = pterm.DefaultSpinner.Start("Building merkle tree...")
	if cbOffset != 0 {
		// last data hash
		hash := calcHash(cb[:cbOffset])
		hashes = append(hashes, hash)

		// save index of file where block starts
		pieces = append(pieces, &PieceInfo{
			StartFileIndex: pieceStartFileIndex,
		})
	}

	hashTree := buildHashTree(hashes)
	for i, piece := range pieces {
		proof, err := hashTree.CreateProof([][]byte{cell.BeginCell().MustStoreSlice(hashes[i], 256).EndCell().Hash()})
		if err != nil {
			return nil, fmt.Errorf("failed to calc proof for piece %d: %w", i, err)
		}
		piece.Proof = proof.ToBOCWithFlags(false)
	}

	torrent.Info = &TorrentInfo{
		PieceSize:  pieceSize,
		FileSize:   uint64(len(headerData)) + dataSize,
		RootHash:   hashTree.Hash(),
		HeaderSize: uint64(len(headerData)),
		HeaderHash: calcHash(headerData),
		Description: tlb.Text{
			MaxFirstChunkSize: tlb.MaxTextChunkSize - 84, // 84 = size of prev data in bytes
			Value:             description,
		},
	}

	tCell, err := tlb.ToCell(torrent.Info)
	if err != nil {
		waiter.Fail(err.Error())
		return nil, err
	}
	waiter.Success("Merkle tree successfully built")

	torrent.BagID = tCell.Hash()
	p := len(pieces) / 8
	if len(pieces)%8 != 0 {
		p++
	}
	torrent.pieceMask = make([]byte, p)

	for i, piece := range pieces {
		err = torrent.setPiece(uint32(i), piece)
		if err != nil {
			return nil, fmt.Errorf("failed to store piece %d in db: %w", i, err)
		}
	}

	torrent.activeFiles = make([]uint32, 0, len(files))
	for i := range files {
		torrent.activeFiles = append(torrent.activeFiles, uint32(i))
	}
	if err = torrent.db.SetActiveFiles(torrent.BagID, torrent.activeFiles); err != nil {
		return nil, fmt.Errorf("failed to store active files in db: %w", err)
	}

	return torrent, nil
}

func buildHashTree(hashes [][]byte) *cell.Cell {
	piecesNum := uint32(len(hashes))
	// calc tree depth
	treeDepth := int(math.Log2(float64(piecesNum)))
	if piecesNum > uint32(math.Pow(2, float64(treeDepth))) {
		// add 1 if pieces num is not exact log2
		treeDepth++
	}

	level := map[uint32]*cell.Cell{}
	for piece := uint32(0); piece < uint32(math.Pow(2, float64(treeDepth))); piece++ {
		var p []byte
		if piece >= uint32(len(hashes)) {
			p = make([]byte, 32)
		} else {
			p = hashes[piece]
		}
		level[piece] = cell.BeginCell().MustStoreSlice(p, 256).EndCell()
	}

	for d := 0; d < treeDepth; d++ {
		nextLevel := map[uint32]*cell.Cell{}
		for k, v := range level {
			isLeft := k&(1<<d) == 0
			nextKey := k
			if !isLeft {
				nextKey = k ^ (1 << d) // switch off bit
			}

			if nextLevel[nextKey] != nil {
				// already processed as neighbour
				continue
			}

			neighbour := level[k^(1<<d)] // get neighbour bit
			b := cell.BeginCell()
			if !isLeft {
				b.MustStoreRef(neighbour)
				b.MustStoreRef(v)
			} else {
				b.MustStoreRef(v)
				b.MustStoreRef(neighbour)
			}

			nextLevel[nextKey] = b.EndCell()
		}
		level = nextLevel
	}
	return level[0]
}

func calcHash(cb []byte) []byte {
	hash := sha256.New()
	hash.Write(cb)
	return hash.Sum(nil)
}

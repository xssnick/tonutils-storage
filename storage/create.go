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
	"os"
	"path/filepath"
	"strings"
)

type fileInfoData struct {
	Path string
	Name string
}

func CreateTorrent(path, description string, db Storage, connector NetConnector) (*Torrent, error) {
	const pieceSize = 128 * 1024

	cb := make([]byte, pieceSize)
	cbOffset := 0

	// strip last slash
	if path[len(path)-1] == '/' || path[len(path)-1] == '\\' {
		path = path[:len(path)-1]
	}

	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	dir := filepath.Base(path) + "/"

	torrent := NewTorrent(filepath.Dir(path), db, connector)
	torrent.Header = &TorrentHeader{
		DirNameSize: uint32(len(dir)),
		DirName:     []byte(dir),
	}

	waiter, _ := pterm.DefaultSpinner.Start("Scanning files...")

	var filesSize uint64
	filesList := make([]fileInfoData, 0, 64)
	err = filepath.Walk(path, func(filePath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		name := filePath[len(path)+1:]
		name = strings.ReplaceAll(name, "\\", "/") // to unix style

		torrent.Header.FilesCount++
		torrent.Header.TotalNameSize += uint64(len(name))
		torrent.Header.Names = append(torrent.Header.Names, name...)
		torrent.Header.NameIndex = append(torrent.Header.NameIndex, torrent.Header.TotalNameSize)

		// TODO: optimize
		// stat is not always gives the right file size, so we open file and find the end
		fl, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filePath, err)
		}

		sz, err := fl.Seek(0, io.SeekEnd)
		fl.Close()
		if err != nil {
			return fmt.Errorf("failed to seek file end %s: %w", filePath, err)
		}
		filesSize += uint64(sz)

		torrent.Header.DataIndex = append(torrent.Header.DataIndex, filesSize)

		// log.Println("Added file ", name, "at", filePath)
		filesList = append(filesList, fileInfoData{
			Path: filePath,
			Name: name,
		})
		return nil
	})
	if err != nil {
		err = fmt.Errorf("failed to scan directory '%s': %w", path, err)
		waiter.Fail(err.Error())
		return nil, err
	}
	waiter.Success()

	files := make([]*FileIndex, 0, 64)
	hashes := make([][]byte, 0, 256)
	pieces := make([]*PieceInfo, 0, 256)

	pieceStartFileIndex := uint32(0)

	process := func(name string, rd io.Reader) error {
		fi := FileIndex{
			Name:            name, // relative path
			BlockFrom:       uint32(len(hashes)),
			BlockFromOffset: uint32(cbOffset),
		}

		for {
			if cbOffset == 0 {
				pieceStartFileIndex = uint32(len(files))
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

		if name != "" { // if not header
			fi.BlockTo = uint32(len(hashes))
			fi.BlockToOffset = uint32(cbOffset)

			files = append(files, &fi)
		}

		return nil
	}

	waiter, _ = pterm.DefaultSpinner.Start("Generating bag header...")
	headerData, err := tl.Serialize(torrent.Header, true)
	if err != nil {
		waiter.Fail(err.Error())
		return nil, fmt.Errorf("failed to serialize header: %w", err)
	}
	err = process("", bytes.NewBuffer(headerData))
	if err != nil {
		waiter.Fail(err.Error())
		return nil, fmt.Errorf("failed to process header piece: %w", err)
	}
	waiter.Success()

	progress, _ := pterm.DefaultProgressbar.WithTotal(len(filesList)).WithTitle("Validating files...").Start()

	// add files
	for _, f := range filesList {
		fl, err := os.Open(f.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", f.Path, err)
		}

		err = process(f.Name, fl)
		_ = fl.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to process file %s: %w", f.Path, err)
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
		FileSize:   uint64(len(headerData)) + filesSize,
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

	for i, file := range files {
		err = torrent.setFileIndex(uint32(i), file)
		if err != nil {
			return nil, fmt.Errorf("failed to store file index %d in db: %w", i, err)
		}
	}

	torrent.activeFiles = make([]uint32, 0, len(filesList))
	for i := range filesList {
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

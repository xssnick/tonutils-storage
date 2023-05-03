package torrent

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/adnl/storage"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

type fileInfo struct {
	Path string
	Name string
}

func CreateTorrent(path, description string) (*Torrent, error) {
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
	header := storage.TorrentHeader{
		DirNameSize: uint32(len(dir)),
		DirName:     []byte(dir),
	}

	waiter, _ := pterm.DefaultSpinner.Start("Scanning torrent files...")

	var filesSize uint64
	filesList := make([]fileInfo, 0, 64)
	err = filepath.Walk(path, func(filePath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		name := filePath[len(path)+1:]
		name = strings.ReplaceAll(name, "\\", "/") // to unix style

		header.FilesCount++
		header.TotalNameSize += uint64(len(name))
		header.Names = append(header.Names, name...)
		header.NameIndex = append(header.NameIndex, header.TotalNameSize)

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

		header.DataIndex = append(header.DataIndex, filesSize)

		// log.Println("Added file ", name, "at", filePath)
		filesList = append(filesList, fileInfo{
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

	blockStartFileIndex := 0

	process := func(name string, rd io.Reader) error {
		fi := FileIndex{
			Name:            name, // relative path
			BlockFrom:       uint32(len(hashes)),
			BlockFromOffset: uint32(cbOffset),
		}

		for {
			if cbOffset == 0 {
				blockStartFileIndex = len(files)
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
					HashForProof:   cell.BeginCell().MustStoreSlice(hash, 256).EndCell().Hash(),
					StartFileIndex: blockStartFileIndex,
				})

				cbOffset = 0
			}
		}
		fi.BlockTo = uint32(len(hashes))
		fi.BlockToOffset = uint32(cbOffset)

		files = append(files, &fi)

		return nil
	}

	waiter, _ = pterm.DefaultSpinner.Start("Generating torrent header...")
	headerData, err := tl.Serialize(&header, true)
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

	progress, _ := pterm.DefaultProgressbar.WithTotal(len(filesList)).WithTitle("Validating torrent files...").Start()

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
			HashForProof:   cell.BeginCell().MustStoreSlice(hash, 256).EndCell().Hash(),
			StartFileIndex: blockStartFileIndex,
		})
	}

	hashTree := buildHashTree(hashes)

	info := storage.TorrentInfo{
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

	tCell, err := tlb.ToCell(info)
	if err != nil {
		waiter.Fail(err.Error())
		return nil, err
	}
	waiter.Success()

	return &Torrent{
		BagID:      tCell.Hash(),
		Path:       path,
		Header:     header,
		Info:       info,
		PieceIndex: pieces,
		Index:      files,
		HashTree:   hashTree,
	}, nil
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

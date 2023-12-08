package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"io"
	"math"
	"runtime"
	"strings"
	"sync"
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

func CreateTorrent(ctx context.Context, filesRootPath, dirName, description string, db Storage, connector NetConnector, files []FileRef) (*Torrent, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("0 files in torrent")
	}
	const pieceSize = 128 * 1024

	cb := make([]byte, pieceSize)
	cbOffset := 0

	if dirName == "/" {
		dirName = ""
	}

	if err := validateFileName(dirName, false); err != nil {
		return nil, err
	}

	torrent := NewTorrent(filesRootPath, db, connector)
	torrent.Header = &TorrentHeader{
		DirNameSize: uint32(len(dirName)),
		DirName:     []byte(dirName),
	}

	waiter, _ := pterm.DefaultSpinner.Start("Scanning files...")

	var dataSize uint64
	for _, file := range files {
		name := file.GetName()

		if err := validateFileName(name, true); err != nil {
			return nil, fmt.Errorf("malicious file name %q: %w", name, err)
		}

		torrent.Header.FilesCount++
		torrent.Header.TotalNameSize += uint64(len(name))
		torrent.Header.Names = append(torrent.Header.Names, name...)
		torrent.Header.NameIndex = append(torrent.Header.NameIndex, torrent.Header.TotalNameSize)

		dataSize += file.GetSize()

		torrent.Header.DataIndex = append(torrent.Header.DataIndex, dataSize)
	}

	filesProcessed := uint32(0)

	hashes := make([][]byte, 0, 256)
	piecesStartIndexes := make([]uint32, 0, 256)

	pieceStartFileIndex := uint32(0)

	process := func(name string, isHeader bool, rd io.Reader, progress *pterm.ProgressbarPrinter) error {
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
				piecesStartIndexes = append(piecesStartIndexes, pieceStartFileIndex)

				cbOffset = 0
				progress.Increment()
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

	fullSz := uint64(len(headerData)) + dataSize
	piecesNum := fullSz / pieceSize
	if fullSz%pieceSize != 0 {
		piecesNum++
	}

	progress, _ := pterm.DefaultProgressbar.WithTotal(int(piecesNum)).WithTitle("Calculating pieces...").Start()

	err = process("", true, bytes.NewBuffer(headerData), progress)
	if err != nil {
		waiter.Fail(err.Error())
		return nil, fmt.Errorf("failed to process header piece: %w", err)
	}
	waiter.Success()

	// add files
	for _, f := range files {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rd, err := f.CreateReader()
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", f.GetName(), err)
		}

		err = process(f.GetName(), false, rd, progress)
		_ = rd.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to process file %s: %w", f.GetName(), err)
		}
	}

	if cbOffset != 0 {
		// last data hash
		hash := calcHash(cb[:cbOffset])
		hashes = append(hashes, hash)

		// save index of file where block starts
		piecesStartIndexes = append(piecesStartIndexes, pieceStartFileIndex)
		progress.Increment()
	}
	_, _ = progress.Stop()

	waiter, _ = pterm.DefaultSpinner.Start("Building merkle tree...")
	hashTree := buildHashTree(hashes)
	waiter.Success("Merkle tree successfully built")

	hashTree.Hash()
	progress, _ = pterm.DefaultProgressbar.WithTotal(int(piecesNum)).WithTitle("Calculating proofs...").Start()

	piecesNum = uint64(len(piecesStartIndexes))
	pcNumBytes := len(piecesStartIndexes) / 8
	if len(piecesStartIndexes)%8 != 0 {
		pcNumBytes++
	}
	torrent.pieceMask = make([]byte, pcNumBytes)

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
	torrent.BagID = tCell.Hash()

	wg := sync.WaitGroup{}
	threads := runtime.NumCPU()
	toCalcErr := make(chan error, threads)
	wg.Add(threads)

	type calcReq struct {
		id         uint32
		startIndex uint32
	}
	toCalc := make(chan *calcReq, threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()

			for {
				var p *calcReq
				select {
				case <-ctx.Done():
					return
				case p = <-toCalc:
					if p == nil {
						return
					}
				}

				err = torrent.setPiece(p.id, &PieceInfo{
					StartFileIndex: p.startIndex,
					Proof:          torrent.fastProof(hashTree, p.id, uint32(piecesNum)).ToBOCWithFlags(false),
				})
				if err != nil {
					toCalcErr <- err
					return
				}
			}
		}()
	}

	for i, idx := range piecesStartIndexes {
		select {
		case <-ctx.Done():
			_, _ = progress.Stop()
			return nil, ctx.Err()
		case err = <-toCalcErr:
			return nil, fmt.Errorf("failed to calc proof for piece: %w", err)
		case toCalc <- &calcReq{id: uint32(i), startIndex: idx}:
			progress.Increment()
		}
	}
	close(toCalc)

	wg.Wait()

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

func (t *Torrent) fastProof(root *cell.Cell, piece, piecesNum uint32) *cell.Cell {
	// calc tree depth
	depth := int(math.Log2(float64(piecesNum)))
	if piecesNum > uint32(math.Pow(2, float64(depth))) {
		// add 1 if pieces num is not exact log2
		depth++
	}

	data := make([]byte, 1+32+2)
	data[0] = 0x03 // merkle proof
	copy(data[1:], root.Hash())
	binary.BigEndian.PutUint16(data[1+32:], uint16(depth))

	proof := cell.BeginCell().MustStoreSlice(data, uint(len(data)*8))

	if depth == 0 {
		// nothing to prune
		proofCell := proof.MustStoreRef(root).EndCell()
		proofCell.UnsafeModify(cell.LevelMask{Mask: 0}, true)
		return proofCell
	}

	type pair struct {
		leftPruned bool
		left       *cell.Builder
		right      *cell.Builder
	}

	var pairs = make([]pair, 0, depth)

	// check bits from left to right and load branches
	for i := depth - 1; i >= 0; i-- {
		isLeft := piece&(1<<i) == 0
		if i == 0 {
			pairs = append(pairs, pair{
				leftPruned: false,
				left:       root.MustPeekRef(0).ToBuilder(),
				right:      root.MustPeekRef(1).ToBuilder(),
			})
			break
		}

		if isLeft {
			pairs = append(pairs, pair{
				leftPruned: false,
				left:       cell.BeginCell(),
				right:      fastPrune(root.MustPeekRef(1), uint16(i)),
			})
			root = root.MustPeekRef(0)
		} else {
			pairs = append(pairs, pair{
				leftPruned: true,
				left:       fastPrune(root.MustPeekRef(0), uint16(i)),
				right:      cell.BeginCell(),
			})
			root = root.MustPeekRef(1)
		}
	}

	newRoot := cell.BeginCell()
	for i := len(pairs) - 1; i >= 0; i-- {
		nextRoot := newRoot
		if i > 0 {
			p := pairs[i-1]
			if !p.leftPruned {
				nextRoot = p.left
			} else {
				nextRoot = p.right
			}
		}

		cll := pairs[i].left.EndCell()
		if i < len(pairs)-2 || (i == len(pairs)-2 && cll.RefsNum() == 0) { // set level only for parents of pruned
			cll.UnsafeModify(cell.LevelMask{Mask: 1}, pairs[i].leftPruned)
		}
		nextRoot.MustStoreRef(cll)

		cll = pairs[i].right.EndCell()
		if i < len(pairs)-2 || (i == len(pairs)-2 && cll.RefsNum() == 0) {
			cll.UnsafeModify(cell.LevelMask{Mask: 1}, !pairs[i].leftPruned)
		}
		nextRoot.MustStoreRef(cll)
	}

	newRootCell := newRoot.EndCell()
	if len(pairs) > 1 {
		newRootCell.UnsafeModify(cell.LevelMask{Mask: 1}, false)
	}

	proofCell := proof.MustStoreRef(newRootCell).EndCell()
	proofCell.UnsafeModify(cell.LevelMask{Mask: 0}, true)

	return proofCell
}

func fastPrune(toPrune *cell.Cell, depth uint16) *cell.Builder {
	prunedData := make([]byte, 2+32+2)
	prunedData[0] = 0x01 // pruned type
	prunedData[1] = 1    // level
	copy(prunedData[2:], toPrune.Hash())
	binary.BigEndian.PutUint16(prunedData[2+32:], depth) //depth
	return cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData)*8))
}

func validateFileName(name string, isFile bool) error {
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("name cannot strat with '/'")
	}
	if strings.Contains(name, "./") {
		return fmt.Errorf("name cannot contain traversal './'")
	}
	if isFile {
		if name == "" {
			return fmt.Errorf("file name cannot be empty")
		}
		if strings.HasSuffix(name, "/") {
			return fmt.Errorf("file name cannot end with /")
		}
	}
	return nil
}

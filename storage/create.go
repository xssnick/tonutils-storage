package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-storage/internal/termui"
	"golang.org/x/sync/errgroup"
)

type FileRef interface {
	GetName() string
	GetSize() uint64
	CreateReader() (io.ReaderAt, func() error, error)
}

type sizedFileRef struct {
	FileRef
	size uint64
}

func (f sizedFileRef) GetSize() uint64 { return f.size }

func CreateTorrent(ctx context.Context, filesRootPath, dirName, description string, db Storage, connector NetConnector, files []FileRef, progressCallback func(done uint64, max uint64)) (*Torrent, error) {
	if dirName == "/" {
		dirName = ""
	}

	if err := validateFileName(dirName, false); err != nil {
		return nil, err
	}
	header := &TorrentHeader{
		DirNameSize: uint32(len(dirName)),
		DirName:     []byte(dirName),
	}

	return CreateTorrentWithInitialHeader(ctx, filesRootPath, description, header, db, connector, files, progressCallback, true)
}

func CreateTorrentWithInitialHeader(ctx context.Context, filesRootPath, description string, header *TorrentHeader, db Storage, connector NetConnector, files []FileRef, progressCallback func(done uint64, max uint64), verbose bool) (*Torrent, error) {

	torrent := NewTorrent(filesRootPath, db, connector)
	torrent.Header = header
	torrent.CreatedLocally = true

	// scanning files to initialize torrent header
	dataSize, err := initializeTorrentHeader(torrent, files, verbose)
	if err != nil {
		return nil, err
	}
	if err = header.validateStatic(); err != nil {
		return nil, fmt.Errorf("invalid torrent header: %w", err)
	}

	var pieceSize = db.GetForcedPieceSize()

	if pieceSize == 0 {
		switch {
		case dataSize > 10<<30: // > 10 GB
			pieceSize = 1 << 20 // 1 MB
		case dataSize > 2<<30: // > 2 GB
			pieceSize = 512 << 10 // 512 KB
		case dataSize > 512<<20: // > 512 MB
			pieceSize = 256 << 10 // 256 KB
		default:
			pieceSize = 128 << 10 // 128 KB
		}
	}

	if pieceSize > 8<<20 {
		return nil, fmt.Errorf("too big piece size")
	}
	var waiter *termui.Spinner
	if verbose {
		waiter, _ = termui.StartSpinner("Generating bag header...")
	}
	headerData, err := tl.Serialize(torrent.Header, true)
	if err != nil {
		if waiter != nil {
			waiter.Fail(err.Error())
		}
		return nil, fmt.Errorf("failed to serialize header: %w", err)
	}

	err = computeHashesAndJoinPieces(ctx, torrent, pieceSize, dataSize, headerData, files, description, waiter, progressCallback)
	if err != nil {
		return nil, err
	}
	torrent.lastVerified = time.Now()

	return torrent, nil
}

// initializeTorrentHeader will perform a scan on torrent files passed and initialize torrent header. Returning the initialized
// torrent and the data size.
func initializeTorrentHeader(torrent *Torrent, files []FileRef, verbose bool) (uint64, error) {
	if torrent.Header == nil {
		return 0, fmt.Errorf("header is nil")
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("0 files in torrent")
	}
	if len(files) > maxTorrentHeaderFiles {
		return 0, fmt.Errorf("too many files in torrent")
	}
	if torrent.Header.FilesCount != 0 || len(torrent.Header.Names) != 0 || len(torrent.Header.NameIndex) != 0 || len(torrent.Header.DataIndex) != 0 {
		return 0, fmt.Errorf("initial header already contains files")
	}
	if err := validateFileName(string(torrent.Header.DirName), false); err != nil {
		return 0, err
	}
	var waiter *termui.Spinner
	if verbose {
		// report on waiter that we are scanning files
		waiter, _ = termui.StartSpinner("Scanning files...")
	}

	var dataSize uint64
	names := make(map[string]struct{}, len(files))
	// iterate over files to build torrent headers
	for _, file := range files {
		name := file.GetName()

		if err := validateFileName(name, true); err != nil {
			return 0, fmt.Errorf("malicious file name %q: %w", name, err)
		}
		cleanName := path.Clean(name)
		if _, exists := names[cleanName]; exists {
			return 0, fmt.Errorf("duplicate file name %q", name)
		}
		names[cleanName] = struct{}{}
		size := file.GetSize()
		if size > math.MaxInt64 || dataSize > math.MaxInt64-size {
			return 0, fmt.Errorf("torrent data size exceeds supported file offsets")
		}

		torrent.Header.FilesCount++
		torrent.Header.TotalNameSize += uint64(len(name))
		torrent.Header.Names = append(torrent.Header.Names, name...)
		torrent.Header.NameIndex = append(torrent.Header.NameIndex, torrent.Header.TotalNameSize)

		dataSize += size

		torrent.Header.DataIndex = append(torrent.Header.DataIndex, dataSize)
	}
	if waiter != nil {
		waiter.Success()
	}

	return dataSize, nil
}

// computeHashesAndJoinPieces
func computeHashesAndJoinPieces(
	ctx context.Context,
	torrent *Torrent,
	pieceSize uint32,
	dataSize uint64,
	headerData []byte,
	files []FileRef,
	description string,
	waiter *termui.Spinner,
	progressCallback func(done uint64, max uint64),
) error {
	if pieceSize == 0 {
		return fmt.Errorf("piece size is zero")
	}
	fullSz, ok := checkedAddUint64(uint64(len(headerData)), dataSize)
	if !ok || fullSz > math.MaxInt64 {
		return fmt.Errorf("torrent size exceeds supported file offsets")
	}
	piecesNum := fullSz / uint64(pieceSize)
	if fullSz%uint64(pieceSize) != 0 {
		piecesNum++
	}
	if piecesNum > math.MaxUint32 {
		return fmt.Errorf("too many pieces in torrent")
	}

	var (
		maxProgress  = piecesNum * 4
		doneProgress uint64
	)
	fileRefs := make([]FileRef, len(files))
	var previousEnd uint64
	for i, file := range files {
		end := torrent.Header.DataIndex[i]
		fileRefs[i] = sizedFileRef{FileRef: file, size: end - previousEnd}
		previousEnd = end
	}

	hashes, piecesStartIndexes, err := computeFileHashes(
		ctx,
		pieceSize,
		headerData,
		fileRefs,
		piecesNum, &doneProgress, maxProgress,
		waiter,
		progressCallback,
	)
	if err != nil {
		return err
	}
	if waiter != nil {
		waiter, _ = termui.StartSpinner("Building merkle tree...")
	}
	hashTree, err := buildMerkleTree(ctx, hashes, 9) // 9 is most efficient in most cases
	if err != nil {
		if waiter != nil {
			waiter.Fail(err.Error())
		}
		return err
	}
	rootHash := hashTree.Hash()
	if waiter != nil {
		waiter.Success("Merkle tree successfully built")
	}
	var progress *termui.ProgressBar
	if waiter != nil {
		progress, _ = termui.StartProgressbar(int(piecesNum), "Calculating proofs...")
	}
	pcNumBytes := len(piecesStartIndexes) / 8
	if len(piecesStartIndexes)%8 != 0 {
		pcNumBytes++
	}
	torrent.pieceMask = make([]byte, pcNumBytes)

	// set torrent stats
	torrent.SetInfoStats(pieceSize, headerData, rootHash, uint64(len(headerData))+dataSize, uint64(len(headerData)), description)

	tCell, err := tlb.ToCell(torrent.Info)
	if err != nil {
		if waiter != nil {
			waiter.Fail(err.Error())
		}
		return err
	}
	torrent.BagID = tCell.Hash()

	err = joinTorrentPieces(ctx, torrent, hashTree, files, piecesStartIndexes, &doneProgress, maxProgress, progress, progressCallback)
	if err != nil {
		return err
	}

	return nil
}

func joinTorrentPieces(
	ctx context.Context,
	torrent *Torrent,
	hashTree *cell.Cell,
	files []FileRef,
	piecesStartIndexes []uint32,
	doneProgress *uint64, maxProgress uint64,
	progress *termui.ProgressBar,
	progressCallback func(done uint64, max uint64),
) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(workerCtx)
	threads := max(1, min(runtime.GOMAXPROCS(0), len(piecesStartIndexes)))
	type calcReq struct {
		id         uint32
		startIndex uint32
	}
	toCalc := make(chan calcReq, threads)
	for range threads {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case request, ok := <-toCalc:
					if !ok {
						return nil
					}
					proof, err := torrent.fastProof(hashTree, request.id, torrent.Info.PiecesNum())
					if err != nil {
						return err
					}
					if err = torrent.setPiece(request.id, &PieceInfo{
						StartFileIndex: request.startIndex,
						Proof:          proof.ToBOCWithFlags(false),
					}, false); err != nil {
						return err
					}
				}
			}
		})
	}

submit:
	for i, index := range piecesStartIndexes {
		select {
		case <-groupCtx.Done():
			break submit
		case toCalc <- calcReq{id: uint32(i), startIndex: index}:
			if progress != nil {
				progress.Increment()
			}
			if progressCallback != nil {
				progressCallback(atomic.AddUint64(doneProgress, 1), maxProgress)
			}
		}
	}
	close(toCalc)
	if progress != nil {
		defer progress.Stop()
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("failed to create piece proofs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	torrent.activeFiles = make([]uint32, 0, len(files))
	for i := range files {
		torrent.activeFiles = append(torrent.activeFiles, uint32(i))
	}
	if err := torrent.db.SetActiveFiles(torrent.BagID, torrent.activeFiles); err != nil {
		return fmt.Errorf("failed to store active files in db: %w", err)
	}

	return nil
}

func computeFileHashes(
	ctx context.Context,
	pieceSize uint32,
	headerData []byte,
	files []FileRef,
	piecesNum uint64, doneProgress *uint64, maxProgress uint64,
	waiter *termui.Spinner,
	progressCallback func(done uint64, max uint64),
) ([]merkleHash, []uint32, error) {
	if pieceSize == 0 || piecesNum == 0 || piecesNum > math.MaxUint32 {
		return nil, nil, fmt.Errorf("invalid piece dimensions")
	}
	hashes := make([]merkleHash, piecesNum)
	indexes := make([]uint32, piecesNum)
	buffer := make([]byte, pieceSize)
	bufferOffset := 0
	var pieceIndex uint64
	var fileIndex, pieceStartFileIndex uint32
	var progress *termui.ProgressBar
	if waiter != nil {
		progress, _ = termui.StartProgressbar(int(piecesNum), "Hashing pieces...")
	}
	if progress != nil {
		defer progress.Stop()
	}
	report := func(count uint64) {
		if progress != nil {
			for range count {
				progress.Increment()
			}
		}
		if progressCallback != nil {
			progressCallback(atomic.AddUint64(doneProgress, count*3), maxProgress)
		}
	}
	process := func(isHeader bool, size uint64, reader io.ReaderAt) error {
		if size > math.MaxInt64 {
			return fmt.Errorf("file size exceeds supported offsets")
		}
		var offset uint64
		for offset < size {
			if err := ctx.Err(); err != nil {
				return err
			}
			if bufferOffset == 0 {
				pieceStartFileIndex = fileIndex
				fullPieces := (size - offset) / uint64(pieceSize)
				if !isHeader && fullPieces > 1 {
					if pieceIndex+fullPieces > piecesNum {
						return fmt.Errorf("source size does not match torrent size")
					}
					if err := hashFullFilePieces(ctx, reader, offset, fullPieces, pieceIndex, fileIndex, pieceSize, hashes, indexes); err != nil {
						return err
					}
					pieceIndex += fullPieces
					offset += fullPieces * uint64(pieceSize)
					report(fullPieces)
					continue
				}
			}
			readSize := min(uint64(len(buffer)-bufferOffset), size-offset)
			n, err := reader.ReadAt(buffer[bufferOffset:bufferOffset+int(readSize)], int64(offset))
			if n != int(readSize) {
				return fmt.Errorf("source size changed: %w", io.ErrUnexpectedEOF)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			offset += uint64(n)
			bufferOffset += n
			if bufferOffset == len(buffer) {
				if pieceIndex >= piecesNum {
					return fmt.Errorf("source size does not match torrent size")
				}
				hashes[pieceIndex] = sha256.Sum256(buffer)
				indexes[pieceIndex] = pieceStartFileIndex
				pieceIndex++
				bufferOffset = 0
				report(1)
			}
		}
		var extra [1]byte
		n, err := reader.ReadAt(extra[:], int64(size))
		if n != 0 || err == nil {
			return fmt.Errorf("source size changed: file grew beyond declared size")
		}
		if !errors.Is(err, io.EOF) {
			return err
		}
		if !isHeader {
			fileIndex++
		}
		return nil
	}
	if err := process(true, uint64(len(headerData)), bytes.NewReader(headerData)); err != nil {
		return nil, nil, fmt.Errorf("failed to hash header: %w", err)
	}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		reader, closeReader, err := file.CreateReader()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open %s: %w", file.GetName(), err)
		}
		err = process(false, file.GetSize(), reader)
		closeErr := closeReader()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to hash %s: %w", file.GetName(), err)
		}
		if closeErr != nil {
			return nil, nil, fmt.Errorf("failed to close %s: %w", file.GetName(), closeErr)
		}
	}
	if bufferOffset > 0 {
		if pieceIndex >= piecesNum {
			return nil, nil, fmt.Errorf("source size does not match torrent size")
		}
		hashes[pieceIndex] = sha256.Sum256(buffer[:bufferOffset])
		indexes[pieceIndex] = pieceStartFileIndex
		pieceIndex++
		report(1)
	}
	if pieceIndex != piecesNum {
		return nil, nil, fmt.Errorf("source size does not match torrent size")
	}
	return hashes, indexes, nil
}

func hashFullFilePieces(ctx context.Context, reader io.ReaderAt, offset, count, firstPiece uint64, fileIndex, pieceSize uint32, hashes []merkleHash, indexes []uint32) error {
	group, workerCtx := errgroup.WithContext(ctx)
	workers := max(1, min(runtime.GOMAXPROCS(0), int(count)))
	jobs := make(chan uint64, workers)
	for range workers {
		group.Go(func() error {
			buffer := make([]byte, pieceSize)
			for {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case index, ok := <-jobs:
					if !ok {
						return nil
					}
					n, err := reader.ReadAt(buffer, int64(offset+index*uint64(pieceSize)))
					if n != len(buffer) {
						return fmt.Errorf("source size changed: %w", io.ErrUnexpectedEOF)
					}
					if err != nil && !errors.Is(err, io.EOF) {
						return err
					}
					hashes[firstPiece+index] = sha256.Sum256(buffer)
					indexes[firstPiece+index] = fileIndex
				}
			}
		})
	}
submit:
	for index := uint64(0); index < count; index++ {
		select {
		case <-workerCtx.Done():
			break submit
		case jobs <- index:
		}
	}
	close(jobs)
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

func calcHash(cb []byte) []byte {
	hash := sha256.Sum256(cb)
	return hash[:]
}

func validateFileName(name string, isFile bool) error {
	if strings.EqualFold(strings.SplitN(name, "/", 2)[0], ".tonutils-storage-pieces") {
		return fmt.Errorf("name uses reserved storage metadata directory")
	}
	if strings.Contains(name, "\x00") {
		return fmt.Errorf("name cannot contain NUL byte")
	}
	if strings.Contains(name, "\\") {
		return fmt.Errorf("name cannot contain backslash")
	}
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("name cannot starts with '/'")
	}
	if isFile {
		if name == "" {
			return fmt.Errorf("file name cannot be empty")
		}
		if strings.HasSuffix(name, "/") {
			return fmt.Errorf("file name cannot end with /")
		}
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" {
			continue
		}
		if part == "." || part == ".." {
			return fmt.Errorf("name cannot contain traversal component %q", part)
		}
	}
	return nil
}

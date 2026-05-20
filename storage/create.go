package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-storage/internal/termui"
)

type FileRef interface {
	GetName() string
	GetSize() uint64
	CreateReader() (io.ReaderAt, func() error, error)
}

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
	if len(files) == 0 {
		return 0, fmt.Errorf("0 files in torrent")
	}
	var waiter *termui.Spinner
	if verbose {
		// report on waiter that we are scanning files
		waiter, _ = termui.StartSpinner("Scanning files...")
	}

	var dataSize uint64
	// iterate over files to build torrent headers
	for _, file := range files {
		name := file.GetName()

		if err := validateFileName(name, true); err != nil {
			return 0, fmt.Errorf("malicious file name %q: %w", name, err)
		}

		torrent.Header.FilesCount++
		torrent.Header.TotalNameSize += uint64(len(name))
		torrent.Header.Names = append(torrent.Header.Names, name...)
		torrent.Header.NameIndex = append(torrent.Header.NameIndex, torrent.Header.TotalNameSize)

		dataSize += file.GetSize()

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
	fullSz := uint64(len(headerData)) + dataSize
	piecesNum := fullSz / uint64(pieceSize)
	if fullSz%uint64(pieceSize) != 0 {
		piecesNum++
	}

	var (
		maxProgress  = piecesNum * 4
		doneProgress uint64
	)

	hashes, piecesStartIndexes, err := computeFileHashes(
		ctx,
		pieceSize,
		headerData,
		files,
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

				proof, err := torrent.fastProof(hashTree, p.id, torrent.Info.PiecesNum())
				if err != nil {
					toCalcErr <- err
					return
				}

				err = torrent.setPiece(p.id, &PieceInfo{
					StartFileIndex: p.startIndex,
					Proof:          proof.ToBOCWithFlags(false),
				}, false)
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
			if progress != nil {
				_ = progress.Stop()
			}
			return ctx.Err()
		case err := <-toCalcErr:
			return fmt.Errorf("failed to calc proof for piece: %w", err)
		case toCalc <- &calcReq{id: uint32(i), startIndex: idx}:
			if progress != nil {
				progress.Increment()
			}
			if progressCallback != nil {
				progressCallback(atomic.AddUint64(doneProgress, 1), maxProgress)
			}
		}
	}
	close(toCalc)

	wg.Wait()

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
	hashes := make([]merkleHash, piecesNum)
	piecesStartFileIndexes := make([]uint32, piecesNum)
	pieceStartFileIndex := uint32(0)
	cb := make([]byte, pieceSize)
	cbOffset := 0
	var filesProcessed uint32
	var piecesProcessed int64

	process := func(isHeader bool, size uint64, rd io.ReaderAt, progress *termui.ProgressBar) error {
		var fileOffset int64 = 0
		end := false
		for !end {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if cbOffset == 0 {
				pieceStartFileIndex = filesProcessed

				// we start parallel execution only for full pieces and when file is big enough
				if !isHeader && size/piecesNum > 10000 {
					type job struct {
						offset int64
						piece  int64
					}

					var done = make(chan error, 130)
					var task = make(chan job, 130)

					ctxWorker, cancelWorker := context.WithCancel(ctx)

					for i := 0; i < runtime.NumCPU(); i++ {
						go func() {
							var buf = make([]byte, pieceSize)
							for {
								select {
								case <-ctxWorker.Done():
									return
								case j := <-task:
									_, err := rd.ReadAt(buf, j.offset)
									if err != nil {
										done <- err
										break
									}

									hashes[j.piece] = sha256.Sum256(buf)
									piecesStartFileIndexes[j.piece] = pieceStartFileIndex

									select {
									case done <- nil:
									case <-ctxWorker.Done():
										return
									}
								}
							}
						}()
					}

					fullPiecesLeft := (int64(size) - fileOffset) / int64(pieceSize)

					go func() {
						for i := int64(0); i < fullPiecesLeft; i++ {
							select {
							case <-ctxWorker.Done():
								return
							case task <- job{
								offset: fileOffset + i*int64(pieceSize),
								piece:  piecesProcessed + i,
							}:
							}
						}
					}()

					// process only full pieces in parallel
					for i := int64(0); i < fullPiecesLeft; i++ {
						select {
						case <-ctx.Done():
							cancelWorker()
							return ctx.Err()
						case err := <-done:
							if err != nil {
								cancelWorker()
								return err
							}
							if progress != nil {
								progress.Increment()
							}
							if progressCallback != nil {
								progressCallback(atomic.AddUint64(doneProgress, 3), maxProgress)
							}
						}
					}

					piecesProcessed += fullPiecesLeft
					fileOffset += fullPiecesLeft * int64(pieceSize)
					cancelWorker()
				}
			}

			n, err := rd.ReadAt(cb[cbOffset:], fileOffset)
			if err != nil {
				if errors.Is(err, io.EOF) {
					end = true
					err = nil
				} else {
					return err
				}
			}
			fileOffset += int64(n)
			cbOffset += n

			if cbOffset == int(pieceSize) {
				hashes[piecesProcessed] = sha256.Sum256(cb)
				// save index of file where block starts
				piecesStartFileIndexes[piecesProcessed] = pieceStartFileIndex

				piecesProcessed++

				cbOffset = 0
				if progress != nil {
					progress.Increment()
				}
				if progressCallback != nil {
					progressCallback(atomic.AddUint64(doneProgress, 3), maxProgress)
				}
			}
		}

		if !isHeader { // if not header
			filesProcessed++
		}
		return nil
	}
	var progress *termui.ProgressBar
	if waiter != nil {
		progress, _ = termui.StartProgressbar(int(piecesNum), "Hashing pieces...")
	}
	err := process(true, uint64(len(headerData)), bytes.NewReader(headerData), progress)
	if err != nil {
		if waiter != nil {
			waiter.Fail(err.Error())
		}
		return nil, nil, fmt.Errorf("failed to process header piece: %w", err)
	}
	if waiter != nil {
		waiter.Success()
	}
	// add files
	for _, f := range files {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		rd, closer, err := f.CreateReader()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read file %s: %w", f.GetName(), err)
		}

		err = process(false, f.GetSize(), rd, progress)
		_ = closer()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process file %s: %w", f.GetName(), err)
		}
	}

	if cbOffset != 0 {
		// last data hash
		hashes[piecesProcessed] = sha256.Sum256(cb[:cbOffset])

		// save index of file where block starts
		piecesStartFileIndexes[piecesProcessed] = pieceStartFileIndex

		piecesProcessed++
		if progress != nil {
			progress.Increment()
		}
		if progressCallback != nil {
			progressCallback(atomic.AddUint64(doneProgress, 3), maxProgress)
		}
	}
	if progress != nil {
		_ = progress.Stop()
	}
	return hashes, piecesStartFileIndexes, nil
}

func calcHash(cb []byte) []byte {
	hash := sha256.Sum256(cb)
	return hash[:]
}

func validateFileName(name string, isFile bool) error {
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

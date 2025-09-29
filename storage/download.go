package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

var DownloadPrefetch = uint32(runtime.NumCPU() * 2 * 16)

type fileInfo struct {
	path string
	info *FileInfo
}

type DownloadResult struct {
	Path        string
	Dir         string
	Description string
}

type Event struct {
	Name  string
	Value any
}

type PiecesInfo struct {
	OverallPieces    int
	PiecesToDownload int
}

const (
	EventErr             = "ERR"
	EventBagResolved     = "BAG_RESOLVED"
	EventFileDownloaded  = "FILE_DOWNLOADED"
	EventDone            = "DONE"
	EventPieceDownloaded = "PIECE_DOWNLOADED"
	EventProgress        = "PROGRESS"
)

func (t *Torrent) prepareDownloader(ctx context.Context) error {
	if t.connector == nil {
		return fmt.Errorf("no connector for torrent")
	}

	var err error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if t.downloader == nil || !t.downloader.IsActive() {
			t.downloader, err = t.connector.CreateDownloader(ctx, t)
			if err != nil {
				Logger("bag information not resolved: %s", err.Error())
				time.Sleep(1 * time.Second)
				continue
			}
		}
		return nil
	}
}

func (t *Torrent) Verify(ctx context.Context, deep bool) (intact bool, err error) {
	tm := time.Now()
	Logger("[VERIFICATION] PREPARING:", hex.EncodeToString(t.BagID))

	intact = true
	if t.Header == nil {
		return intact, nil
	}

	rootPath := filepath.Join(t.Path, string(t.Header.DirName))
	checked := make([]bool, t.Info.PiecesNum())

	var files []uint32
	if t.downloadAll {
		for i := uint32(0); i < t.Header.FilesCount; i++ {
			files = append(files, i)
		}
	} else {
		files = t.GetActiveFilesIDs()
	}

	Logger("[VERIFICATION] STARTED:", hex.EncodeToString(t.BagID), "FILES:", len(files), "DOWNLOAD ALL:", t.downloadAll)

	needDownload := false
	for _, f := range files {
		info, err := t.GetFileOffsetsByID(f)
		if err != nil {
			Logger("[VERIFICATION] GET FILE ID ERR:", hex.EncodeToString(t.BagID), err.Error())

			continue
		}

		path := filepath.Join(rootPath, info.Name)

		isDelete := false
		if !t.db.GetFS().Exists(path) {
			isDelete = true
			Logger("[VERIFICATION] FAILED FOR FILE:", path, "BAG:", hex.EncodeToString(t.BagID), "Not exists")
		} else if deep {
			if info.Size > 256*1024*1024 {
				type jobRes struct {
					piece uint32
					err   error
				}
				type job struct {
					piece uint32
					res   chan jobRes
				}

				jobs := make(chan job, 100)
				results := make(chan jobRes, 100)
				ctxWorker, cancel := context.WithCancel(ctx)

				workers := (runtime.NumCPU() / 3) * 2
				if workers == 0 {
					workers = 1
				}

				for i := 0; i < workers; i++ {
					go func() {
						for {
							select {
							case <-ctxWorker.Done():
								return
							case j := <-jobs:
								_, err := t.getPieceInternal(j.piece, true)
								select {
								case <-ctxWorker.Done():
									return
								case j.res <- jobRes{piece: j.piece, err: err}:
								}
							}
						}
					}()
				}

				go func() {
					for i := info.FromPiece; i <= info.ToPiece; i++ {
						if checked[i] {
							continue
						}

						jobs <- job{piece: i, res: results}
					}
				}()

				for i := info.FromPiece; i <= info.ToPiece; i++ {
					if checked[i] {
						continue
					}

					select {
					case <-ctxWorker.Done():
						cancel()
						return false, ctxWorker.Err()
					case res := <-results:
						if res.err != nil {
							if !strings.Contains(res.err.Error(), "is not downloaded") {
								Logger("[VERIFICATION] FAILED FOR PIECE:", res.piece, "BAG:", hex.EncodeToString(t.BagID), res.err.Error())
								isDelete = true
							}
						}

						// mark only for existing pieces to remove not only 1 file in not exist
						checked[res.piece] = res.err == nil
					}

					if isDelete {
						break
					}
				}
				cancel()
			} else {
				for i := info.FromPiece; i <= info.ToPiece; i++ {
					if checked[i] {
						continue
					}

					if ctx.Err() != nil {
						return false, ctx.Err()
					}

					_, err := t.getPieceInternal(i, true)
					if err != nil {
						if strings.Contains(err.Error(), "is not downloaded") {
							continue
						}

						isDelete = true
						break
					}

					// mark only for existing pieces to remove not only 1 file in not exist
					checked[i] = true
				}
			}
		}

		if isDelete {
			intact = false
			// we delete whole file because size can be > than expected
			// and just replace of piece will be not enough
			for i := info.FromPiece; i <= info.ToPiece; i++ {
				// file was deleted, delete pieces records also
				if err = t.removePiece(i); err != nil {
					Logger("[VERIFICATION] REMOVE PIECE ERR:", hex.EncodeToString(t.BagID), i, err.Error())

					return false, err
				}
			}

			if !t.CreatedLocally {
				needDownload = true
				Logger("[VERIFICATION] NEED DOWNLOAD:", path, hex.EncodeToString(t.BagID))

				if err = t.db.GetFS().Delete(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
					Logger("[VERIFICATION] FAILED TO REMOVE FILE:", path, hex.EncodeToString(t.BagID), err.Error())
				}
			} else {
				Logger("[VERIFICATION] CORRUPTED, BUT CREATED LOCALLY AND WE WILL NOT TOUCH FILES :", hex.EncodeToString(t.BagID))
			}
		}
	}

	if needDownload {
		// restart download of missing pieces
		currFlag := t.currentDownloadFlag
		currPause := t.pause
		_ = t.startDownload(func(event Event) {
			if event.Name == EventErr && currFlag == t.currentDownloadFlag {
				currPause()
			}
		})
	}

	Logger("[VERIFICATION] COMPLETED:", hex.EncodeToString(t.BagID), "NEED DOWNLOAD:", needDownload, "INTACT:", intact, "TOOK:", time.Since(tm).String())

	return intact, nil
}

func (t *Torrent) startDownload(report func(Event)) error {
	if t.BagID == nil {
		return fmt.Errorf("bag is not set")
	}

	// we use flag pointer to know is download was replaced
	var flag = false
	t.currentDownloadFlag = &flag

	stop := t.stopDownload
	if stop != nil {
		// stop current download
		stop()
	}
	var ctx context.Context
	ctx, stop = context.WithCancel(t.globalCtx)
	t.stopDownload = stop

	t.opWg.Add(1)
	go func() {
		defer t.opWg.Done()
		defer func() {
			if stop != nil {
				stop()
			}
		}()

		piecesMap := map[uint32]bool{}
		var list []fileInfo

		if t.Header == nil || t.Info == nil {
			if err := t.prepareDownloader(ctx); err != nil {
				Logger("failed to prepare downloader for", hex.EncodeToString(t.BagID), "err: ", err.Error())
				return
			}
		}

		// update torrent in db
		if err := t.db.SetTorrent(t); err != nil {
			Logger("failed to set torrent in db", hex.EncodeToString(t.BagID), "err: ", err.Error())
			return
		}

		var downloaded uint64
		rootPath := filepath.Join(t.Path, string(t.Header.DirName))

		var files []uint32
		if t.downloadAll {
			for i := uint32(0); i < t.Header.FilesCount; i++ {
				files = append(files, i)
			}
		} else {
			files = t.GetActiveFilesIDs()
			if len(files) == 0 {
				// do not stop download because we just loaded headers
				// TODO: make it better
				t.stopDownload = nil
			}
		}

		list = make([]fileInfo, 0, len(files))
		for _, f := range files {
			info, err := t.GetFileOffsetsByID(f)
			if err != nil {
				continue
			}

			needFile := false

			if !t.db.GetFS().Exists(filepath.Join(rootPath, info.Name)) {
				needFile = true
				for i := info.FromPiece; i <= info.ToPiece; i++ {
					piecesMap[i] = true
					// file was deleted, delete pieces records also
					_ = t.removePiece(i)
				}
			} else {
				for i := info.FromPiece; i <= info.ToPiece; i++ {
					// TODO: read file parts and compare with hashes
					if _, err = t.getPiece(i); err != nil {
						needFile = true
						piecesMap[i] = true
						continue
					}
					downloaded++
				}
			}

			if needFile {
				list = append(list, fileInfo{info: info, path: info.Name})
			}
		}

		pieces := make([]byte, t.Info.PiecesNum())
		for p := range piecesMap {
			pieces[p] = 1
		}

		sort.Slice(list, func(i, j int) bool {
			return uint64(list[i].info.ToPiece)<<32+uint64(list[i].info.ToPieceOffset) <
				uint64(list[j].info.ToPiece)<<32+uint64(list[j].info.ToPieceOffset)
		})

		report(Event{Name: EventBagResolved, Value: PiecesInfo{OverallPieces: int(t.Info.PiecesNum()), PiecesToDownload: len(piecesMap)}})
		if len(pieces) > 0 {
			if err := t.prepareDownloader(ctx); err != nil {
				Logger("failed to prepare downloader for", hex.EncodeToString(t.BagID), "err: ", err.Error())
				return
			}

			if t.downloadOrdered {
				fetch := NewPreFetcher(ctx, t, report, DownloadPrefetch, pieces)
				defer fetch.Stop()

				if err := writeOrdered(ctx, t, list, piecesMap, rootPath, report, fetch); err != nil {
					report(Event{Name: EventErr, Value: err})
					return
				}
			} else {
				filesMap := map[uint32]bool{}
				for _, file := range files {
					filesMap[file] = true
				}

				left := len(pieces)
				ready := make(chan uint32, DownloadPrefetch)
				fetch := NewPreFetcher(ctx, t, func(event Event) {
					if event.Name == EventPieceDownloaded {
						ready <- event.Value.(uint32)
					}
					report(event)
				}, DownloadPrefetch, pieces)
				defer fetch.Stop()

				var currentFile FSFile
				var currentFileId uint32

				for i := 0; i < left; i++ {
					select {
					case e := <-ready:
						err := func(piece uint32) error {
							currentPiece, currentProof, err := fetch.WaitGet(ctx, piece)
							if err != nil {
								return fmt.Errorf("failed to download piece %d: %w", piece, err)
							}
							defer fetch.Free(piece)

							pieceFiles, err := t.GetFilesInPiece(piece)
							if err != nil {
								return fmt.Errorf("failed to get files of piece %d: %w", piece, err)
							}

							for _, file := range pieceFiles {
								if !filesMap[file.Index] {
									continue
								}

								if err := validateFileName(file.Name, true); err != nil {
									Logger(fmt.Sprintf("Malicious file '%s' was skipped: %v", file.Name, err))
									continue
								}

								err = func() error {
									if currentFile == nil || currentFileId != file.Index {
										if currentFile != nil {
											currentFile.Close()
										}

										for x := 1; x <= 5; x++ {
											// we retry because on Windows close file behaves
											// like async, and it may throw that file still opened
											currentFile, err = t.db.GetFS().Open(filepath.Join(rootPath, file.Name), OpenModeWrite)
											if err != nil {
												Logger(fmt.Errorf("failed to create or open file %s: %w", file.Name, err).Error())
												time.Sleep(time.Duration(x*50) * time.Millisecond)
												continue
											}
											currentFileId = file.Index
											break
										}
										if err != nil {
											return fmt.Errorf("failed to create or open file %s: %w", file.Name, err)
										}
									}

									notEmptyFile := file.FromPiece != file.ToPiece || file.FromPieceOffset != file.ToPieceOffset
									if notEmptyFile {
										fileOff := int64(0)
										if file.FromPiece != piece {
											fileOff = int64(piece-file.FromPiece)*int64(t.Info.PieceSize) - int64(file.FromPieceOffset)
										}

										data := currentPiece
										if file.ToPiece == piece {
											data = data[:file.ToPieceOffset]
										}
										if file.FromPiece == piece {
											data = data[file.FromPieceOffset:]
										}

										_, err = currentFile.WriteAt(data, fileOff)
										if err != nil {
											return fmt.Errorf("failed to write file %s: %w", file.Name, err)
										}
									}

									return currentFile.Sync()
								}()
								if err != nil {
									return err
								}
							}

							if i == left-1 && currentFile != nil {
								currentFile.Close()
							}

							err = t.setPiece(piece, &PieceInfo{
								StartFileIndex: pieceFiles[0].Index,
								Proof:          currentProof,
							}, false)
							if err != nil {
								return fmt.Errorf("failed to save piece %d to db: %w", piece, err)
							}

							return nil
						}(e)
						if err != nil {
							report(Event{Name: EventErr, Value: err})
							return
						}
					case <-ctx.Done():
						report(Event{Name: EventErr, Value: ctx.Err()})
						return
					}
				}
			}
		}

		report(Event{Name: EventDone, Value: DownloadResult{
			Path:        rootPath,
			Dir:         string(t.Header.DirName),
			Description: t.Info.Description.Value,
		}})

		for id := range t.GetPeers() {
			peerId, _ := hex.DecodeString(id)
			t.ResetDownloadPeer(peerId)
		}

		if len(files) == 0 {
			// stop if downloaded failed, on header we leave it for reuse
			stop = nil
		}
	}()

	return nil
}

func writeOrdered(ctx context.Context, t *Torrent, list []fileInfo, piecesMap map[uint32]bool, rootPath string, report func(Event), fetch *PreFetcher) error {
	var currentPieceId uint32
	var pieceStartFileIndex uint32
	var currentPiece, currentProof []byte
	for _, off := range list {
		err := func() error {
			if strings.Contains(off.path, "..") {
				Logger("Malicious file with path traversal was skipped: " + off.path)
				return fmt.Errorf("malicious file")
			}
			if err := validateFileName(off.path, true); err != nil {
				Logger(fmt.Sprintf("Malicious file '%s' was skipped: %v", off.path, err))
				return fmt.Errorf("malicious file %q", off.path)
			}

			f, err := t.db.GetFS().Open(filepath.Join(rootPath, off.path), OpenModeWrite)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", off.path, err)
			}
			defer f.Close()

			notEmptyFile := off.info.FromPiece != off.info.ToPiece || off.info.FromPieceOffset != off.info.ToPieceOffset
			if notEmptyFile {
				for piece := off.info.FromPiece; piece <= off.info.ToPiece; piece++ {
					if !piecesMap[piece] {
						continue
					}

					if piece != currentPieceId || currentPiece == nil {
						if currentPiece != nil {
							fetch.Free(currentPieceId)

							err = t.setPiece(currentPieceId, &PieceInfo{
								StartFileIndex: pieceStartFileIndex,
								Proof:          currentProof,
							}, false)
							if err != nil {
								return fmt.Errorf("failed to save piece %d to db: %w", currentPieceId, err)
							}
						}

						pieceStartFileIndex = off.info.Index
						currentPiece, currentProof, err = fetch.WaitGet(ctx, piece)
						if err != nil {
							return fmt.Errorf("failed to download piece %d: %w", piece, err)
						}

						currentPieceId = piece
					}
					part := currentPiece
					offset := int64(piece-off.info.FromPiece) * int64(t.Info.PieceSize)
					if piece == off.info.ToPiece {
						part = part[:off.info.ToPieceOffset]
					}
					if piece == off.info.FromPiece {
						part = part[off.info.FromPieceOffset:]
					}
					if piece > off.info.FromPiece {
						offset -= int64(off.info.FromPieceOffset)
					}

					if piece < off.info.FromPiece || piece > off.info.ToPiece {
						// assert, should never happen
						panic("piece is not related to file")
					}

					_, err = f.WriteAt(part, offset)
					if err != nil {
						return fmt.Errorf("failed to write piece %d for file %s: %w", piece, off.path, err)
					}
				}
			}

			report(Event{Name: EventFileDownloaded, Value: off.path})
			return nil
		}()
		if err != nil {
			return err
		}
	}

	if currentPiece != nil {
		fetch.Free(currentPieceId)

		err := t.setPiece(currentPieceId, &PieceInfo{
			StartFileIndex: pieceStartFileIndex,
			Proof:          currentProof,
		}, false)
		if err != nil {
			return fmt.Errorf("failed to save piece %d to db: %w", currentPieceId, err)
		}
	}
	return nil
}

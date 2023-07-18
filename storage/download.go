package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

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
			t.downloader, err = t.connector.CreateDownloader(ctx, t, 5, 12)
			if err != nil {
				Logger("bag information not resolved: %s", err.Error())
				time.Sleep(1 * time.Second)
				continue
			}
		}
		return nil
	}
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

	go func() {
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

			// update torrent in db
			if err := t.db.SetTorrent(t); err != nil {
				Logger("failed to set torrent in db", hex.EncodeToString(t.BagID), "err: ", err.Error())
				return
			}
		}

		var downloaded uint64
		rootPath := t.Path + "/" + string(t.Header.DirName)

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

			if !t.db.GetFS().Exists(rootPath + "/" + info.Name) {
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

		pieces := make([]uint32, 0, len(piecesMap))
		for p := range piecesMap {
			pieces = append(pieces, p)
		}

		sort.Slice(pieces, func(i, j int) bool {
			return pieces[i] < pieces[j]
		})
		sort.Slice(list, func(i, j int) bool {
			return uint64(list[i].info.ToPiece)<<32+uint64(list[i].info.ToPieceOffset) <
				uint64(list[j].info.ToPiece)<<32+uint64(list[j].info.ToPieceOffset)
		})

		report(Event{Name: EventBagResolved, Value: PiecesInfo{OverallPieces: int(t.PiecesNum()), PiecesToDownload: len(pieces)}})
		if len(pieces) > 0 {
			if err := t.prepareDownloader(ctx); err != nil {
				Logger("failed to prepare downloader for", hex.EncodeToString(t.BagID), "err: ", err.Error())
				return
			}

			if t.downloadOrdered {
				fetch := NewPreFetcher(ctx, t, t.downloader, report, downloaded, 24, 200, pieces)
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
				ready := make(chan uint32, 200)
				fetch := NewPreFetcher(ctx, t, t.downloader, func(event Event) {
					if event.Name == EventPieceDownloaded {
						ready <- event.Value.(uint32)
					}
					report(event)
				}, downloaded, 24, 200, pieces)
				defer fetch.Stop()

				var currentFile FSFile
				var currentFileId uint32

				defer func() {
					if currentFile != nil {
						currentFile.Close()
					}
				}()

				for i := 0; i < left; i++ {
					select {
					case e := <-ready:
						err := func(piece uint32) error {
							currentPiece, currentProof, err := fetch.Get(ctx, piece)
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
											currentFile, err = t.db.GetFS().Open(rootPath+"/"+file.Name, OpenModeWrite)
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
										fileOff := uint32(0)
										if file.FromPiece != piece {
											fileOff = (piece-file.FromPiece)*t.Info.PieceSize - file.FromPieceOffset
										}

										data := currentPiece
										if file.ToPiece == piece {
											data = data[:file.ToPieceOffset]
										}
										if file.FromPiece == piece {
											data = data[file.FromPieceOffset:]
										}

										_, err = currentFile.WriteAt(data, int64(fileOff))
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

							err = t.setPiece(piece, &PieceInfo{
								StartFileIndex: pieceFiles[0].Index,
								Proof:          currentProof,
							})
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

			f, err := t.db.GetFS().Open(rootPath+"/"+off.path, OpenModeWrite)
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
							})
							if err != nil {
								return fmt.Errorf("failed to save piece %d to db: %w", currentPieceId, err)
							}
						}

						pieceStartFileIndex = off.info.Index
						currentPiece, currentProof, err = fetch.Get(ctx, piece)
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
		})
		if err != nil {
			return fmt.Errorf("failed to save piece %d to db: %w", currentPieceId, err)
		}
	}
	return nil
}

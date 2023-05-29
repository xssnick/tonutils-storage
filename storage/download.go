package storage

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
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
			t.downloader, err = t.connector.CreateDownloader(ctx, t, 5, 20, math.MaxUint32)
			if err != nil {
				Logger("bag information not resolved: %s", err.Error())
				time.Sleep(1 * time.Second)
				continue
			}
		}
		return nil
	}
}

func (t *Torrent) startDownload(report func(Event), downloadAll, downloadOrdered bool) error {
	if t.BagID == nil {
		return fmt.Errorf("bag is not set")
	}

	t.mx.Lock()
	if t.stopDownload != nil {
		// stop current download
		t.stopDownload()
	}
	var ctx context.Context
	ctx, t.stopDownload = context.WithCancel(t.globalCtx)
	t.mx.Unlock()

	go func() {
		piecesMap := map[uint32]bool{}
		var list []fileInfo

		if t.Header == nil || t.Info == nil {
			if err := t.prepareDownloader(ctx); err != nil {
				return
			}
		}

		var downloaded uint64
		rootPath := t.Path + "/" + string(t.Header.DirName)

		var files []uint32
		if downloadAll {
			for i := uint32(0); i < t.Header.FilesCount; i++ {
				files = append(files, i)
			}
		} else {
			files = t.GetActiveFilesIDs()
		}

		list = make([]fileInfo, 0, len(files))
		for _, f := range files {
			info, err := t.GetFileOffsetsByID(f)
			if err != nil {
				continue
			}

			needFile := false

			_, err = os.Stat(rootPath + "/" + info.Name)
			if err != nil {
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
				return
			}

			if downloadOrdered {
				fetch := NewPreFetcher(ctx, t, report, downloaded, 20, 200, 0, pieces)
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
				fetch := NewPreFetcher(ctx, t, func(event Event) {
					if event.Name == EventPieceDownloaded {
						ready <- event.Value.(uint32)
					}
					report(event)
				}, downloaded, 20, 200, 0, pieces)
				defer fetch.Stop()

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

								err = func() error {
									if err := os.MkdirAll(filepath.Dir(rootPath+"/"+file.Name), os.ModePerm); err != nil {
										return err
									}

									f, err := os.OpenFile(rootPath+"/"+file.Name, os.O_RDWR|os.O_CREATE, 0666)
									if err != nil {
										return fmt.Errorf("failed to create file %s: %w", file.Name, err)
									}
									defer f.Close()

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

										_, err = f.WriteAt(data, int64(fileOff))
										if err != nil {
											return fmt.Errorf("failed to write file %s: %w", file.Name, err)
										}

										err = t.setFileIndex(file.Index, &FileIndex{
											BlockFrom:       file.FromPiece,
											BlockTo:         file.ToPiece,
											BlockFromOffset: file.FromPieceOffset,
											BlockToOffset:   file.ToPieceOffset,
											Name:            file.Name,
										})
										if err != nil {
											return fmt.Errorf("failed to write file index for %s: %w", file.Name, err)
										}
									}

									return nil
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
	}()

	return nil
}

func writeOrdered(ctx context.Context, t *Torrent, list []fileInfo, piecesMap map[uint32]bool, rootPath string, report func(Event), fetch *PreFetcher) error {
	var currentPieceId uint32
	var pieceStartFileIndex uint32
	var currentPiece, currentProof []byte
	for _, off := range list {
		err := func() error {
			if err := os.MkdirAll(filepath.Dir(rootPath+"/"+off.path), os.ModePerm); err != nil {
				return err
			}

			f, err := os.OpenFile(rootPath+"/"+off.path, os.O_RDWR|os.O_CREATE, 0666)
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

							if err = f.Sync(); err != nil {
								return fmt.Errorf("failed to sync file for piece %d: %w", currentPieceId, err)
							}

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

			err = t.setFileIndex(off.info.Index, &FileIndex{
				BlockFrom:       off.info.FromPiece,
				BlockTo:         off.info.ToPiece,
				BlockFromOffset: off.info.FromPieceOffset,
				BlockToOffset:   off.info.ToPieceOffset,
				Name:            off.info.Name,
			})
			if err != nil {
				return fmt.Errorf("failed to write file index for %s: %w", off.info.Name, err)
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

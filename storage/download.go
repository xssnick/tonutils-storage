package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl"
	"math"
	"os"
	"path/filepath"
	"sort"
)

type Downloader struct {
	client *Client
}

func NewDownloader(dht DHT) *Downloader {
	return &Downloader{
		client: NewClient(dht),
	}
}

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

func (d *Downloader) Download(t *Torrent, gate *adnl.Gateway, bag []byte, onlyHeader bool, report chan<- Event, files []string) func() {
	ctx, stopper := context.WithCancel(context.Background())
	go func() {
		if t.BagID != nil && !bytes.Equal(t.BagID, bag) {
			report <- Event{Name: EventErr, Value: fmt.Errorf("bag not matches torrent: %s %s",
				hex.EncodeToString(t.BagID), hex.EncodeToString(bag))}
			return
		}
		t.BagID = bag

		dn, err := d.client.createDownloader(ctx, t, gate, 5, 20, math.MaxUint32)
		if err != nil {
			report <- Event{Name: EventErr, Value: fmt.Errorf("bag information not resolved: %w", err)}
			return
		}
		defer dn.Close()

		piecesMap := map[uint32]bool{}
		list := make([]fileInfo, 0, len(files))

		var downloaded uint64
		var rootPath string
		if !onlyHeader {
			rootPath = t.Path + "/" + string(t.Header.DirName)
			if len(files) == 0 {
				files, err = t.ListFiles()
				if err != nil {
					report <- Event{Name: EventErr, Value: fmt.Errorf("failed to list files: %w", err)}
					return
				}
			}

			for _, f := range files {
				info, err := t.GetFileOffsets(f)
				if err != nil {
					continue
				}

				needFile := false

				_, err = os.Stat(rootPath + "/" + f)
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
					list = append(list, fileInfo{info: info, path: f})
				}
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

		report <- Event{Name: EventBagResolved, Value: PiecesInfo{OverallPieces: int(t.piecesNum()), PiecesToDownload: len(pieces)}}
		if onlyHeader {
			return
		}

		fetch := NewPreFetcher(ctx, t, dn, report, downloaded, 20, 200, 0, pieces)
		defer fetch.Stop()

		if err = writeOrdered(ctx, t, list, piecesMap, rootPath, report, fetch); err != nil {
			report <- Event{Name: EventErr, Value: err}
			return
		}

		report <- Event{Name: EventDone, Value: DownloadResult{
			Path:        rootPath,
			Dir:         string(t.Header.DirName),
			Description: t.Info.Description.Value,
		}}
	}()

	return stopper
}

func writeOrdered(ctx context.Context, t *Torrent, list []fileInfo, piecesMap map[uint32]bool, rootPath string, report chan<- Event, fetch *PreFetcher) error {
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

			report <- Event{Name: EventFileDownloaded, Value: off.path}
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

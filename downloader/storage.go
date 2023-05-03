package downloader

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/adnl/storage"
	"os"
	"path/filepath"
	"sort"
)

type Downloader struct {
	client *storage.Client
}

func NewDownloader(dht storage.DHT) *Downloader {
	return &Downloader{
		client: storage.NewClient(dht),
	}
}

type fileInfo struct {
	path string
	info *storage.FileInfo
}

type DownloadResult struct {
	Path        string
	Dir         string
	Description string
}

func (d *Downloader) Download(ctx context.Context, path string, bag []byte) (*DownloadResult, error) {
	sp, _ := pterm.DefaultSpinner.WithText("Resolving torrent information...").Start()
	dn, err := d.client.CreateDownloader(ctx, bag, 1, 50)
	if err != nil {
		sp.Fail("Torrent information not resolved: " + err.Error())
		return nil, err
	}
	defer dn.Close()

	tPath := path + "/" + hex.EncodeToString(bag) + "/" + dn.GetDirName()

	names := dn.ListFiles()
	list := make([]fileInfo, 0, len(names))
	for _, f := range names {
		list = append(list, fileInfo{info: dn.GetFileOffsets(f), path: f})
	}

	sort.Slice(list, func(i, j int) bool {
		return uint64(list[i].info.ToPiece)<<32+uint64(list[i].info.ToPieceOffset) <
			uint64(list[j].info.ToPiece)<<32+uint64(list[j].info.ToPieceOffset)
	})

	first, last := list[0].info.FromPiece, list[len(list)-1].info.ToPiece
	num := last - first

	sp.Success("Torrent information resolved")
	p, _ := pterm.DefaultProgressbar.WithTotal(int(num)).WithTitle("Downloading bag").Start()

	fetch := NewPreFetcher(ctx, dn, p, 50, 500, list[0].info.FromPiece, list[len(list)-1].info.ToPiece)
	defer fetch.Close()

	var currentPieceId uint32
	var currentPiece []byte
	for _, off := range list {
		err = func() error {
			if err := os.MkdirAll(filepath.Dir(tPath+"/"+off.path), os.ModePerm); err != nil {
				return err
			}

			// p.UpdateTitle("Downloading " + off.path) // Update the title of the progressbar.

			f, err := os.Create(tPath + "/" + off.path)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", off.path, err)
			}
			defer f.Close()

			if off.info.FromPiece != off.info.ToPiece || off.info.FromPieceOffset != off.info.ToPieceOffset {
				for piece := off.info.FromPiece; piece <= off.info.ToPiece; piece++ {
					// pOff := piece - off.info.FromPiece
					if piece != currentPieceId || currentPiece == nil {
						if currentPiece != nil {
							fetch.Free(currentPieceId)
						}

						currentPiece, err = fetch.Get(ctx, piece)
						if err != nil {
							return fmt.Errorf("failed to download piece %d: %w", piece, err)
						}
						/*p.Increment()
						p.UpdateTitle("Downloading " + off.path + " pieces [" + fmt.Sprint(pOff) + "/" + fmt.Sprint(off.info.ToPiece-off.info.FromPiece) + "] " +
							fmt.Sprint(fetch.BytesPerSecond()/1024) + " KB/s")*/

						currentPieceId = piece
					}
					part := currentPiece
					if piece == off.info.ToPiece {
						part = part[:off.info.ToPieceOffset]
					}
					if piece == off.info.FromPiece {
						part = part[off.info.FromPieceOffset:]
					}

					_, err = f.Write(part)
					if err != nil {
						return fmt.Errorf("failed to write piece %d for file %s: %w", piece, off.path, err)
					}
				}
			}

			pterm.Success.Println("Downloaded " + off.path)
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	return &DownloadResult{
		Path:        tPath,
		Dir:         dn.GetDirName(),
		Description: dn.GetDescription(),
	}, nil
}

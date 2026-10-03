package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strconv"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const cachedPieceStartFileIndex = ^uint32(0)

// Boundary pieces contain bytes of files that were not selected for download.
// Keep their complete verified payload so those pieces can still be seeded.
func (t *Torrent) cachedPiecePath(id uint32) string {
	return filepath.Join(t.Path, ".tonutils-storage-pieces", hex.EncodeToString(t.BagID), strconv.FormatUint(uint64(id), 10))
}

func (t *Torrent) readCachedPiece(id uint32) ([]byte, bool, error) {
	if t.db == nil || t.db.GetFS() == nil || t.Info == nil {
		return nil, false, nil
	}
	fileSystem := t.db.GetFS()
	name := t.cachedPiecePath(id)
	if !fileSystem.Exists(name) {
		return nil, false, nil
	}
	start := uint64(id) * uint64(t.Info.PieceSize)
	if start >= t.Info.FileSize {
		return nil, true, fmt.Errorf("cached piece %d is out of range", id)
	}
	size := min(uint64(t.Info.PieceSize), t.Info.FileSize-start)
	data := make([]byte, int(size))
	n, err := fileSystem.GetController().AcquireRead(name, data, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, true, fmt.Errorf("failed to read cached piece %d: %w", id, err)
	}
	if n != len(data) {
		return nil, true, fmt.Errorf("cached piece %d is incomplete: %w", id, io.ErrUnexpectedEOF)
	}
	return data, true, nil
}

func (t *Torrent) cleanupCachedPiece(id uint32) error {
	if t.db == nil || t.db.GetFS() == nil {
		return nil
	}
	err := t.db.GetFS().Delete(t.cachedPiecePath(id))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (t *Torrent) cacheBoundaryPiece(id uint32, data []byte, selected map[uint32]bool) (bool, error) {
	files, err := t.GetFilesInPiece(id)
	if err != nil {
		return false, err
	}
	for _, file := range files {
		if !selected[file.Index] {
			return true, t.writeCachedPiece(id, data)
		}
	}
	return false, nil
}

func (t *Torrent) writeCachedPiece(id uint32, data []byte) error {
	name := t.cachedPiecePath(id)
	fileSystem := t.db.GetFS()
	f, err := fileSystem.Open(name, OpenModeWrite)
	if err != nil {
		return fmt.Errorf("failed to open cache for piece %d: %w", id, err)
	}
	defer f.Close()
	if err = resizeDownloadFile(fileSystem, f, name, int64(len(data))); err != nil {
		return err
	}
	n, err := f.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("failed to cache piece %d: %w", id, err)
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync cache for piece %d: %w", id, err)
	}
	return nil
}

func resizeDownloadFile(fileSystem FS, file FSFile, name string, size int64) error {
	if resizer, ok := fileSystem.(interface{ Resize(string, int64) error }); ok {
		return resizer.Resize(name, size)
	}
	if resizer, ok := file.(interface{ Truncate(int64) error }); ok {
		return resizer.Truncate(size)
	}
	return nil
}

func (t *Torrent) prepareDownloadFile(root string, info *FileInfo) error {
	name := filepath.Join(root, info.Name)
	fileSystem := t.db.GetFS()
	f, err := fileSystem.Open(name, OpenModeWrite)
	if err != nil {
		return fmt.Errorf("failed to create or open file %s: %w", info.Name, err)
	}
	defer f.Close()
	if err = resizeDownloadFile(fileSystem, f, name, int64(info.Size)); err != nil {
		return fmt.Errorf("failed to resize file %s: %w", info.Name, err)
	}
	if info.Size == 0 {
		return f.Sync()
	}
	return nil
}

type downloadCacheReader struct {
	id   uint32
	data []byte
}

func (c *downloadCacheReader) read(t *Torrent, id uint32, piece *PieceInfo) ([]byte, error) {
	if c.data != nil && c.id == id {
		return c.data, nil
	}
	data, found, err := t.readCachedPiece(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("cache for piece %d is missing", id)
	}
	proof, err := cell.FromBOC(piece.Proof)
	if err != nil {
		return nil, err
	}
	if err = cell.CheckProof(proof, t.Info.RootHash); err != nil {
		return nil, err
	}
	if err = t.checkProofBranch(proof, data, id); err != nil {
		return nil, err
	}
	c.id, c.data = id, data
	return data, nil
}

func (t *Torrent) planDownloadFile(ctx context.Context, root string, info *FileInfo, pieces map[uint32]bool, cache *downloadCacheReader) (bool, error) {
	name := filepath.Join(root, info.Name)
	existed := t.db.GetFS().Exists(name)
	if err := t.prepareDownloadFile(root, info); err != nil {
		return false, err
	}
	if info.Size == 0 {
		return false, nil
	}
	var destination FSFile
	defer func() {
		if destination != nil {
			_ = destination.Close()
		}
	}()
	needsDownload := false
	for id := info.FromPiece; id <= info.ToPiece; id++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		piece, err := t.getPiece(id)
		if err == nil && piece.StartFileIndex == cachedPieceStartFileIndex {
			data, cacheErr := cache.read(t, id, piece)
			if cacheErr == nil {
				if destination == nil {
					destination, err = t.db.GetFS().Open(name, OpenModeWrite)
					if err != nil {
						return false, err
					}
				}
				from, to := uint32(0), uint32(len(data))
				if id == info.FromPiece {
					from = info.FromPieceOffset
				}
				if id == info.ToPiece {
					to = info.ToPieceOffset
				}
				offset := int64(id-info.FromPiece)*int64(t.Info.PieceSize) - int64(info.FromPieceOffset) + int64(from)
				n, writeErr := destination.WriteAt(data[from:to], offset)
				if writeErr != nil {
					return false, writeErr
				}
				if n != int(to-from) {
					return false, io.ErrShortWrite
				}
				continue
			}
			// A missing or corrupt generated cache can be downloaded again.
			if err = t.removePiece(id); err != nil {
				return false, err
			}
			err = cacheErr
		} else if err == nil && !existed {
			if err = t.removePiece(id); err != nil {
				return false, err
			}
			err = fs.ErrNotExist
		}
		if err != nil {
			needsDownload = true
			pieces[id] = true
		}
	}
	if destination != nil {
		if err := destination.Sync(); err != nil {
			return false, err
		}
	}
	return needsDownload, nil
}

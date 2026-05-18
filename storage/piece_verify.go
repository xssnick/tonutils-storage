package storage

import (
	"context"
	"fmt"
	"path/filepath"
)

type PieceProofStatus byte

const (
	PieceProofStatusOK PieceProofStatus = iota
	PieceProofStatusMismatch
	PieceProofStatusMissing
)

type PieceProofFailure struct {
	Piece uint32
	Err   error
}

type PieceProofReport struct {
	TotalPieces   uint32
	OKPieces      uint32
	MissingPieces uint32
	CheckedFiles  uint32
	MissingFiles  []string
	Failed        []PieceProofFailure
	Statuses      []PieceProofStatus
}

func (t *Torrent) CheckPiecesProofs(ctx context.Context) (*PieceProofReport, error) {
	if t.Info == nil || t.Header == nil {
		return nil, fmt.Errorf("bag metadata is not loaded")
	}

	t.InitMask()
	mask := t.PiecesMask()
	total := t.Info.PiecesNum()

	report := &PieceProofReport{
		TotalPieces: total,
		Statuses:    make([]PieceProofStatus, total),
	}

	for id := uint32(0); id < total; id++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}

		if !bitsetHas(mask, id) {
			report.Statuses[id] = PieceProofStatusMissing
			report.MissingPieces++
			continue
		}

		if _, err := t.getPieceInternal(id, true); err != nil {
			report.Statuses[id] = PieceProofStatusMismatch
			report.Failed = append(report.Failed, PieceProofFailure{
				Piece: id,
				Err:   err,
			})
			continue
		}

		report.Statuses[id] = PieceProofStatusOK
		report.OKPieces++
	}

	files, err := t.expectedLocalFiles()
	if err != nil {
		return report, err
	}
	report.CheckedFiles = uint32(len(files))

	rootPath := filepath.Join(t.Path, string(t.Header.DirName))
	for _, file := range files {
		path := filepath.Join(rootPath, file.Name)
		if !t.db.GetFS().Exists(path) {
			report.MissingFiles = append(report.MissingFiles, path)
		}
	}

	return report, nil
}

func (t *Torrent) expectedLocalFiles() ([]*FileInfo, error) {
	var fileIDs []uint32
	if t.downloadAll {
		fileIDs = make([]uint32, 0, t.Header.FilesCount)
		for i := uint32(0); i < t.Header.FilesCount; i++ {
			fileIDs = append(fileIDs, i)
		}
	} else {
		fileIDs = append([]uint32(nil), t.GetActiveFilesIDs()...)
	}

	files := make([]*FileInfo, 0, len(fileIDs))
	for _, id := range fileIDs {
		info, err := t.GetFileOffsetsByID(id)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve file %d: %w", id, err)
		}
		files = append(files, info)
	}
	return files, nil
}

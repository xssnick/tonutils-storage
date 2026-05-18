package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTorrent_CheckPiecesProofs(t *testing.T) {
	tmp := t.TempDir()
	rootPath := filepath.Join(tmp, "bags")
	dirName := "bag"
	filePath := filepath.Join(rootPath, dirName, "file.bin")

	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	data := []byte("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	store := newE2EStorage(16)
	tor, err := CreateTorrent(context.Background(), rootPath, dirName+"/", "test bag", store, nil, []FileRef{
		e2eFileRef{
			path: filePath,
			name: "file.bin",
			size: uint64(len(data)),
		},
	}, nil)
	if err != nil {
		t.Fatalf("failed to create torrent: %v", err)
	}

	info, err := tor.GetFileOffsetsByID(0)
	if err != nil {
		t.Fatalf("failed to resolve file offsets: %v", err)
	}
	if info.FromPiece == info.ToPiece {
		t.Fatalf("expected file to span multiple pieces, got one piece %d", info.FromPiece)
	}

	badPiece := info.FromPiece
	missingPiece := info.ToPiece

	data[0] ^= 0xFF
	if err = os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatalf("failed to rewrite temp file: %v", err)
	}
	if err = tor.removePiece(missingPiece); err != nil {
		t.Fatalf("failed to remove piece: %v", err)
	}

	report, err := tor.CheckPiecesProofs(context.Background())
	if err != nil {
		t.Fatalf("unexpected verify error: %v", err)
	}

	if report.TotalPieces != tor.Info.PiecesNum() {
		t.Fatalf("unexpected total pieces: got %d want %d", report.TotalPieces, tor.Info.PiecesNum())
	}
	if report.Statuses[badPiece] != PieceProofStatusMismatch {
		t.Fatalf("expected piece %d to mismatch, got %v", badPiece, report.Statuses[badPiece])
	}
	if report.Statuses[missingPiece] != PieceProofStatusMissing {
		t.Fatalf("expected piece %d to be missing, got %v", missingPiece, report.Statuses[missingPiece])
	}
	if len(report.Failed) != 1 {
		t.Fatalf("expected one failed piece, got %d", len(report.Failed))
	}
	if report.Failed[0].Piece != badPiece {
		t.Fatalf("expected failed piece %d, got %d", badPiece, report.Failed[0].Piece)
	}
	if report.MissingPieces != 1 {
		t.Fatalf("expected one missing piece, got %d", report.MissingPieces)
	}
	if report.OKPieces == 0 {
		t.Fatal("expected at least one ok piece")
	}
}

func TestTorrent_CheckPiecesProofs_MissingFilesListed(t *testing.T) {
	tmp := t.TempDir()
	rootPath := filepath.Join(tmp, "bags")
	dirName := "bag"
	filePath := filepath.Join(rootPath, dirName, "missing.bin")

	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	data := []byte(strings.Repeat("x", 64))
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	store := newE2EStorage(16)
	tor, err := CreateTorrent(context.Background(), rootPath, dirName+"/", "test bag", store, nil, []FileRef{
		e2eFileRef{
			path: filePath,
			name: "missing.bin",
			size: uint64(len(data)),
		},
	}, nil)
	if err != nil {
		t.Fatalf("failed to create torrent: %v", err)
	}

	if err = os.Remove(filePath); err != nil {
		t.Fatalf("failed to remove temp file: %v", err)
	}

	report, err := tor.CheckPiecesProofs(context.Background())
	if err != nil {
		t.Fatalf("unexpected verify error: %v", err)
	}

	if len(report.MissingFiles) != 1 {
		t.Fatalf("expected one missing file, got %d", len(report.MissingFiles))
	}
	if report.MissingFiles[0] != filePath {
		t.Fatalf("unexpected missing file path: got %s want %s", report.MissingFiles[0], filePath)
	}
	if report.CheckedFiles != 1 {
		t.Fatalf("expected one checked file, got %d", report.CheckedFiles)
	}
}

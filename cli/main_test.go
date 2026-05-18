package main

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xssnick/tonutils-storage/storage"
)

func TestRenderVerifyPieceLines(t *testing.T) {
	lines := renderVerifyPieceLines([]storage.PieceProofStatus{
		storage.PieceProofStatusOK,
		storage.PieceProofStatusOK,
		storage.PieceProofStatusMismatch,
		storage.PieceProofStatusMissing,
		storage.PieceProofStatusOK,
	}, 3)

	expected := []string{
		"0-2 [..!]",
		"3-4 [?.]",
	}
	if !reflect.DeepEqual(lines, expected) {
		t.Fatalf("unexpected lines: got %#v want %#v", lines, expected)
	}
}

func TestBagDisplayName(t *testing.T) {
	tor := &storage.Torrent{
		Header: &storage.TorrentHeader{
			DirName: []byte("photos/"),
		},
	}

	if got := bagDisplayName(tor); got != "photos" {
		t.Fatalf("unexpected bag name: got %q want %q", got, "photos")
	}
}

func TestParseVerifyArgs(t *testing.T) {
	tests := []struct {
		name      string
		parts     []string
		wantBagID string
		wantFiles bool
	}{
		{name: "missing", parts: []string{"verify"}},
		{name: "bag only", parts: []string{"verify", "bag"}, wantBagID: "bag"},
		{name: "bag files", parts: []string{"verify", "bag", "files"}, wantBagID: "bag", wantFiles: true},
		{name: "files bag", parts: []string{"verify", "files", "bag"}, wantBagID: "bag", wantFiles: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBagID, gotFiles := parseVerifyArgs(tt.parts)
			if gotBagID != tt.wantBagID || gotFiles != tt.wantFiles {
				t.Fatalf("unexpected parse result: got (%q, %v) want (%q, %v)", gotBagID, gotFiles, tt.wantBagID, tt.wantFiles)
			}
		})
	}
}

func TestParseVerifyAllWorkers(t *testing.T) {
	tests := []struct {
		name        string
		parts       []string
		wantWorkers int
		wantErr     bool
	}{
		{name: "default", parts: []string{"verify_all"}, wantWorkers: 1},
		{name: "custom", parts: []string{"verify_all", "8"}, wantWorkers: 8},
		{name: "zero", parts: []string{"verify_all", "0"}, wantErr: true},
		{name: "bad", parts: []string{"verify_all", "abc"}, wantErr: true},
		{name: "too many", parts: []string{"verify_all", "2", "x"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWorkers, err := parseVerifyAllWorkers(tt.parts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("unexpected error state: err=%v wantErr=%v", err, tt.wantErr)
			}
			if gotWorkers != tt.wantWorkers {
				t.Fatalf("unexpected workers: got %d want %d", gotWorkers, tt.wantWorkers)
			}
		})
	}
}

func TestBuildVerifyAllCSVRows(t *testing.T) {
	rows := buildVerifyAllCSVRows([]verifyAllResult{
		{BagID: "bag1", Name: "alpha", CreatedAt: time.Date(2026, 4, 16, 10, 11, 12, 0, time.UTC), TotalPieces: 12, DamagedPieces: 3},
		{BagID: "bag2", Name: "beta", TotalPieces: 7, DamagedPieces: 1},
	})

	expected := [][]string{
		{"bag_id", "name", "created_at", "total_pieces", "damaged_pieces"},
		{"bag1", "alpha", "2026-04-16T10:11:12Z", "12", "3"},
		{"bag2", "beta", "", "7", "1"},
	}
	if !reflect.DeepEqual(rows, expected) {
		t.Fatalf("unexpected rows: got %#v want %#v", rows, expected)
	}
}

func TestCollectVerifyFailedPieceFiles(t *testing.T) {
	tor := &storage.Torrent{
		Path: "/bags",
		Info: &storage.TorrentInfo{
			PieceSize:  10,
			HeaderSize: 0,
			FileSize:   20,
		},
		Header: &storage.TorrentHeader{
			FilesCount:    2,
			TotalNameSize: uint64(len("a/one.txtb/two.txt")),
			DirNameSize:   uint32(len("album/")),
			DirName:       []byte("album/"),
			DataIndex:     []uint64{10, 20},
			NameIndex:     []uint64{9, 18},
			Names:         []byte("a/one.txtb/two.txt"),
		},
	}

	files, err := collectVerifyFailedPieceFiles(tor, &storage.PieceProofReport{
		Failed: []storage.PieceProofFailure{
			{Piece: 0},
			{Piece: 1},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{
		filepath.Join("/bags", "album", "a/one.txt"),
		filepath.Join("/bags", "album", "b/two.txt"),
	}
	if !reflect.DeepEqual(files, expected) {
		t.Fatalf("unexpected files: got %#v want %#v", files, expected)
	}
}

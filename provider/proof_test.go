package provider

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-storage/storage"
)

func providerProofTree(t *testing.T, pieces uint32) (*cell.Cell, []byte) {
	t.Helper()
	leaves := uint32(1)
	for leaves < pieces {
		leaves *= 2
	}
	cells := make([]*cell.Cell, leaves)
	for i := range cells {
		var hash [sha256.Size]byte
		if uint32(i) < pieces {
			hash = sha256.Sum256([]byte(fmt.Sprintf("piece %d", i)))
		}
		cells[i] = cell.BeginCell().MustStoreSlice(hash[:], 256).EndCell()
	}
	for len(cells) > 1 {
		parents := make([]*cell.Cell, len(cells)/2)
		for i := range parents {
			parents[i] = cell.BeginCell().MustStoreRef(cells[2*i]).MustStoreRef(cells[2*i+1]).EndCell()
		}
		cells = parents
	}
	skeleton := cell.CreateProofSkeleton()
	skeleton.SetRecursive()
	proof, err := cells[0].CreateProof(skeleton)
	if err != nil {
		t.Fatal(err)
	}
	return cells[0], proof.ToBOC()
}

func TestVerifyStorageProofIncludesPartialPiece(t *testing.T) {
	const pieceSize = 4096
	for _, tc := range []struct {
		name string
		size uint64
	}{
		{name: "small bag", size: 1000},
		{name: "exact piece", size: pieceSize},
		{name: "partial second piece", size: pieceSize + 1},
		{name: "three pieces", size: 2*pieceSize + 1},
		{name: "four pieces", size: 3*pieceSize + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &storage.TorrentInfo{FileSize: tc.size, PieceSize: pieceSize}
			tree, proof := providerProofTree(t, info.PiecesNum())
			info.RootHash = tree.Hash()
			for offset := uint64(0); offset < tc.size; offset += pieceSize {
				if err := verifyStorageProof(info, offset, proof); err != nil {
					t.Fatalf("valid proof for byte %d rejected: %v", offset, err)
				}
			}
			if err := verifyStorageProof(info, tc.size-1, proof); err != nil {
				t.Fatalf("valid proof for last byte rejected: %v", err)
			}
		})
	}
}

func TestVerifyStorageProofRejectsInvalidProof(t *testing.T) {
	tree, proof := providerProofTree(t, 1)
	info := &storage.TorrentInfo{PieceSize: 4096, FileSize: 1, RootHash: tree.Hash()}
	wrongRoot := *info
	wrongRoot.RootHash = bytes.Repeat([]byte{1}, 32)
	for _, tc := range []struct {
		name      string
		info      *storage.TorrentInfo
		proofByte uint64
		proof     []byte
	}{
		{name: "invalid BOC", info: info, proof: []byte("bad proof")},
		{name: "wrong root", info: &wrongRoot, proof: proof},
		{name: "byte past end", info: info, proofByte: 1, proof: proof},
		{name: "missing metadata", proof: proof},
		{name: "zero piece size", info: &storage.TorrentInfo{FileSize: 1, RootHash: tree.Hash()}, proof: proof},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := verifyStorageProof(tc.info, tc.proofByte, tc.proof); err == nil {
				t.Fatal("invalid proof accepted")
			}
		})
	}
}

func TestVerifyStorageProofRejectsMissingBranch(t *testing.T) {
	tree, _ := providerProofTree(t, 4)
	skeleton := cell.CreateProofSkeleton()
	skeleton.ProofRef(0).SetRecursive()
	proof, err := tree.CreateProof(skeleton)
	if err != nil {
		t.Fatal(err)
	}
	info := &storage.TorrentInfo{PieceSize: 4096, FileSize: 3*4096 + 1, RootHash: tree.Hash()}
	if err := verifyStorageProof(info, 0, proof.ToBOC()); err != nil {
		t.Fatalf("proof for revealed piece rejected: %v", err)
	}
	if err := verifyStorageProof(info, 2*4096, proof.ToBOC()); err == nil {
		t.Fatal("proof without the requested piece accepted")
	}
}

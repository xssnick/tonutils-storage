package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMerkleTreeRootAndProofsSmallTrees(t *testing.T) {
	for size := 1; size <= 96; size++ {
		t.Run(fmt.Sprintf("pieces_%d", size), func(t *testing.T) {
			assertMerkleTreeProofs(t, size, rangeInts(0, size)...)
		})
	}
}

func TestMerkleTreeRootAndProofsBoundarySizes(t *testing.T) {
	for _, size := range []int{
		97, 127, 128, 129,
		255, 256, 257,
		511, 512, 513,
		1023, 1024, 1025,
		2047, 2048, 2049,
		4095, 4096, 4097,
	} {
		t.Run(fmt.Sprintf("pieces_%d", size), func(t *testing.T) {
			assertMerkleTreeProofs(t, size, proofSamplePieces(size)...)
		})
	}
}

func TestMerkleTreeUsageProofParityWithLegacySkeleton(t *testing.T) {
	for _, size := range []int{1, 2, 3, 4, 7, 8, 9, 31, 32, 33, 127, 128, 129, 1024, 1025} {
		t.Run(fmt.Sprintf("pieces_%d", size), func(t *testing.T) {
			hashes := testMerkleHashes(size)
			tree, err := buildMerkleTree(context.Background(), hashes, 4)
			if err != nil {
				t.Fatalf("failed to build tree: %v", err)
			}

			torrent := &Torrent{}
			for _, piece := range proofSamplePieces(size) {
				usageProof, err := torrent.fastProof(tree, uint32(piece), uint32(size))
				if err != nil {
					t.Fatalf("failed to build usage proof for piece %d: %v", piece, err)
				}
				legacyProof, err := buildLegacyProofForTest(tree, uint32(piece), uint32(size))
				if err != nil {
					t.Fatalf("failed to build legacy proof for piece %d: %v", piece, err)
				}

				if got, want := usageProof.ToBOCWithFlags(false), legacyProof.ToBOCWithFlags(false); !bytes.Equal(got, want) {
					t.Fatalf("proof BOC mismatch for piece %d:\ngot  %x\nwant %x", piece, got, want)
				}
			}
		})
	}
}

func TestMerkleTreeRejectsBadInputs(t *testing.T) {
	if _, err := buildMerkleTree(context.Background(), nil, 4); err == nil {
		t.Fatal("expected empty tree error")
	}

	tree, err := buildMerkleTree(context.Background(), testMerkleHashes(2), 4)
	if err != nil {
		t.Fatalf("failed to build tree: %v", err)
	}

	torrent := &Torrent{}
	if _, err = torrent.fastProof(tree, 2, 2); err == nil {
		t.Fatal("expected out of range proof error")
	}
	if _, err = torrent.fastProof(nil, 0, 1); err == nil {
		t.Fatal("expected nil root proof error")
	}
}

func buildLegacyProofForTest(root *cell.Cell, piece, piecesNum uint32) (*cell.Cell, error) {
	depth, _, err := merkleTreeShape(int(piecesNum))
	if err != nil {
		return nil, err
	}

	skeleton := cell.CreateProofSkeleton()
	cursor := skeleton
	for bit := int(depth) - 1; bit >= 1; bit-- {
		cursor = cursor.ProofRef(int((piece >> uint(bit)) & 1))
	}
	cursor.SetRecursive()

	return root.CreateProof(skeleton)
}

func assertMerkleTreeProofs(t *testing.T, size int, pieces ...int) {
	t.Helper()

	hashes := testMerkleHashes(size)
	tree, err := buildMerkleTree(context.Background(), hashes, 4)
	if err != nil {
		t.Fatalf("failed to build tree: %v", err)
	}

	depth, _, err := merkleTreeShape(size)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tree.Depth(0), depth; got != want {
		t.Fatalf("root depth mismatch: got %d want %d", got, want)
	}

	torrent := &Torrent{}
	rootHash := tree.Hash(0)
	for _, piece := range pieces {
		proof, err := torrent.fastProof(tree, uint32(piece), uint32(size))
		if err != nil {
			t.Fatalf("failed to build proof for piece %d: %v", piece, err)
		}
		if err = cell.CheckProof(proof, rootHash); err != nil {
			t.Fatalf("proof check failed for piece %d: %v", piece, err)
		}
		assertProofLeaf(t, proof, rootHash, uint32(piece), size, hashes[piece])
	}
}

func assertProofLeaf(t *testing.T, proof *cell.Cell, rootHash []byte, piece uint32, pieces int, want merkleHash) {
	t.Helper()

	body, err := cell.UnwrapProof(proof, rootHash)
	if err != nil {
		t.Fatalf("failed to unwrap proof: %v", err)
	}

	depth, _, err := merkleTreeShape(pieces)
	if err != nil {
		t.Fatal(err)
	}

	node := body
	for bit := int(depth) - 1; bit >= 0; bit-- {
		ref := int((piece >> uint(bit)) & 1)
		node, err = node.PeekRef(ref)
		if err != nil {
			t.Fatalf("failed to walk proof at bit %d: %v", bit, err)
		}
	}

	leaf, err := node.BeginParse()
	if err != nil {
		t.Fatalf("failed to parse proof leaf: %v", err)
	}
	bits, data, err := leaf.RestBits()
	if err != nil {
		t.Fatalf("failed to read proof leaf: %v", err)
	}
	if bits != merkleHashBits {
		t.Fatalf("leaf bits mismatch: got %d want %d", bits, merkleHashBits)
	}
	if !bytes.Equal(data, want[:]) {
		t.Fatalf("leaf hash mismatch: got %x want %x", data, want)
	}
}

func testMerkleHashes(size int) []merkleHash {
	hashes := make([]merkleHash, size)
	for i := range hashes {
		var seed [16]byte
		binary.BigEndian.PutUint64(seed[:8], uint64(size))
		binary.BigEndian.PutUint64(seed[8:], uint64(i))
		hashes[i] = sha256.Sum256(seed[:])
	}
	return hashes
}

func proofSamplePieces(size int) []int {
	candidates := []int{
		0,
		1,
		size/2 - 1,
		size / 2,
		size/2 + 1,
		size - 2,
		size - 1,
	}
	out := make([]int, 0, len(candidates))
	seen := map[int]struct{}{}
	for _, piece := range candidates {
		if piece < 0 || piece >= size {
			continue
		}
		if _, ok := seen[piece]; ok {
			continue
		}
		seen[piece] = struct{}{}
		out = append(out, piece)
	}
	return out
}

func rangeInts(from, to int) []int {
	if to <= from {
		return nil
	}
	out := make([]int, to-from)
	for i := range out {
		out[i] = from + i
	}
	return out
}

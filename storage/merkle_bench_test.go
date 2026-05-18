package storage

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

const benchmarkProofPieces = 100_000

var (
	benchmarkHashesOnce sync.Once
	benchmarkHashes     []merkleHash
	benchmarkBytesSink  int
	benchmarkHashSink   []byte
)

func BenchmarkMerkleTreeBuild100k(b *testing.B) {
	hashes := benchmarkMillionHashes()

	for i := 0; i < b.N; i++ {
		tree, err := buildMerkleTree(context.Background(), hashes, 9)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkHashSink = tree.Hash(0)
	}
}

func BenchmarkProofGenerationAllPieces100k(b *testing.B) {
	hashes := benchmarkMillionHashes()
	tree, err := buildMerkleTree(context.Background(), hashes, 9)
	if err != nil {
		b.Fatal(err)
	}
	torrent := &Torrent{}
	runtime.GC()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		total := 0
		for piece := range hashes {
			proof, err := torrent.fastProof(tree, uint32(piece), uint32(len(hashes)))
			if err != nil {
				b.Fatal(err)
			}
			total += len(proof.ToBOCWithFlags(false))
		}
		benchmarkBytesSink = total
	}
}

func benchmarkMillionHashes() []merkleHash {
	benchmarkHashesOnce.Do(func() {
		benchmarkHashes = testMerkleHashes(benchmarkProofPieces)
	})
	return benchmarkHashes
}

package storage

import (
	"crypto/rand"
	"testing"
)

func BenchmarkBuildMerkleTree(b *testing.B) {
	hashes := make([][]byte, 1_000_000)
	for i := 0; i < len(hashes); i++ {
		hashes[i] = make([]byte, 32)
		rand.Read(hashes[i])
	}

	var hash []byte
	for i := 0; i < b.N; i++ {
		hash = buildMerkleTree(hashes).Hash(0)
	}
	_ = hash
}

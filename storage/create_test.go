package storage

import (
	"crypto/rand"
	"io"
	"os"
	"testing"
)

func BenchmarkBuildMerkleTreeDepth10_1kk(b *testing.B) {
	testTree(10, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth9_1kk(b *testing.B) {
	testTree(9, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth8_1kk(b *testing.B) {
	testTree(8, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth7_1kk(b *testing.B) {
	testTree(7, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth6_1kk(b *testing.B) {
	testTree(6, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth5_1kk(b *testing.B) {
	testTree(5, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth4_1kk(b *testing.B) {
	testTree(4, 1_000_000, b)
}
func BenchmarkBuildMerkleTreeDepth3_1kk(b *testing.B) {
	testTree(3, 1_000_000, b)
}

func BenchmarkBuildMerkleTreeDepth10_500k(b *testing.B) {
	testTree(10, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth9_500k(b *testing.B) {
	testTree(9, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth8_500k(b *testing.B) {
	testTree(8, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth7_500k(b *testing.B) {
	testTree(7, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth6_500k(b *testing.B) {
	testTree(6, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth5_500k(b *testing.B) {
	testTree(5, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth4_500k(b *testing.B) {
	testTree(4, 500_000, b)
}
func BenchmarkBuildMerkleTreeDepth3_500k(b *testing.B) {
	testTree(3, 500_000, b)
}

func BenchmarkBuildMerkleTreeDepth10_100k(b *testing.B) {
	testTree(10, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth9_100k(b *testing.B) {
	testTree(9, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth8_100k(b *testing.B) {
	testTree(8, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth7_100k(b *testing.B) {
	testTree(7, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth6_100k(b *testing.B) {
	testTree(6, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth5_100k(b *testing.B) {
	testTree(5, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth4_100k(b *testing.B) {
	testTree(4, 100_000, b)
}
func BenchmarkBuildMerkleTreeDepth3_100k(b *testing.B) {
	testTree(3, 100_000, b)
}

func BenchmarkBuildMerkleTreeDepth10_10k(b *testing.B) {
	testTree(10, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth9_10k(b *testing.B) {
	testTree(9, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth8_10k(b *testing.B) {
	testTree(8, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth7_10k(b *testing.B) {
	testTree(7, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth6_10k(b *testing.B) {
	testTree(6, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth5_10k(b *testing.B) {
	testTree(5, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth4_10k(b *testing.B) {
	testTree(4, 10_000, b)
}
func BenchmarkBuildMerkleTreeDepth3_10k(b *testing.B) {
	testTree(3, 10_000, b)
}

func BenchmarkBuildMerkleTreeDepth10_1k(b *testing.B) {
	testTree(10, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth9_1k(b *testing.B) {
	testTree(9, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth8_1k(b *testing.B) {
	testTree(8, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth7_1k(b *testing.B) {
	testTree(7, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth6_1k(b *testing.B) {
	testTree(6, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth5_1k(b *testing.B) {
	testTree(5, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth4_1k(b *testing.B) {
	testTree(4, 1_000, b)
}
func BenchmarkBuildMerkleTreeDepth3_1k(b *testing.B) {
	testTree(3, 1_000, b)
}

type fileRef struct {
	file *os.File
	size uint64
}

func (f fileRef) GetName() string {
	return f.file.Name()
}

func (f fileRef) GetSize() uint64 {
	return f.size
}

func (f fileRef) CreateReader() (io.ReaderAt, func() error, error) {
	return f.file, func() error {
		return f.file.Close()
	}, nil
}

func TestInitializeTorrent(t *testing.T) {
	// create tmp files
	err := os.Mkdir("dumpfiles", 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("dumpfiles")

	files := make([]FileRef, 1000)
	buff := make([]byte, 1024)

	for i := 0; i < 1000; i++ {
		f, err := os.CreateTemp("dumpfiles", "dump_")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		_, err = rand.Read(buff)
		if err != nil {
			t.Fatal(err)
		}

		f.Write(buff)

		files[i] = fileRef{
			file: f,
			size: 1024,
		}
	}

	torrent := &Torrent{
		Header: &TorrentHeader{
			DirNameSize: uint32(len("dumpfiles")),
			DirName:     []byte("dumpfiles"),
		},
	}

	dataSize, err := initializeTorrentHeader(torrent, files, true)
	if err != nil {
		t.Fatal(err)
	}

	if dataSize != 1024*1000 {
		t.Fatalf("expected 1024 * 1000, got : %d", dataSize)
	}

	if torrent.Header.FilesCount != 1000 {
		t.Fatal("invalid files count")
	}

	if len(torrent.Header.DataIndex) != 1000 {
		t.Fatal("invalid torrent.Header.DataIndex size")
	}
}

func testTree(depth int, hashesCount int, b *testing.B) {
	hashes := createHashes(hashesCount)
	var hash []byte
	for i := 0; i < b.N; i++ {
		hash = buildMerkleTree(hashes, depth).Hash(0)
	}
	_ = hash
}

func createHashes(size int) [][]byte {
	hashes := make([][]byte, size)
	for i := 0; i < len(hashes); i++ {
		hashes[i] = make([]byte, 32)
		rand.Read(hashes[i])
	}
	return hashes
}

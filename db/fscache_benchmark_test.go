package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Sparse files avoid writing gigabytes during setup; full-size reads exercise
// the cache with the 128 KiB and 1 MiB pieces used by torrent creation.
func BenchmarkFDCacheWorkingSet(b *testing.B) {
	root := b.TempDir()
	paths := make([]string, 5000)
	for i := range paths {
		paths[i] = filepath.Join(root, fmt.Sprint(i))
		file, err := os.Create(paths[i])
		if err != nil {
			b.Fatal(err)
		}
		if err := file.Truncate(1 << 20); err != nil {
			_ = file.Close()
			b.Fatal(err)
		}
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
	}
	for _, pieceSize := range []int{128 << 10, 1 << 20} {
		for _, workingSet := range []int{800, 5000} {
			b.Run(fmt.Sprintf("bytes_%d/files_%d", pieceSize, workingSet), func(b *testing.B) {
				previous := CachedFDLimit
				CachedFDLimit = 800
				cache := NewFSControllerCache(false)
				b.Cleanup(func() {
					cache.lifecycleMx.Lock()
					for path := range cache.dsc {
						cache.invalidate(path)
					}
					cache.lifecycleMx.Unlock()
					CachedFDLimit = previous
				})
				buffer := make([]byte, pieceSize)
				for _, path := range paths[workingSet-min(workingSet, CachedFDLimit) : workingSet] {
					if _, err := cache.AcquireRead(path, buffer[:1], 0); err != nil {
						b.Fatal(err)
					}
				}
				index := 0
				b.ReportAllocs()
				b.SetBytes(int64(pieceSize))
				b.ResetTimer()
				for b.Loop() {
					if n, err := cache.AcquireRead(paths[index%workingSet], buffer, 0); err != nil || n != len(buffer) {
						b.Fatalf("read %d/%d bytes: %v", n, len(buffer), err)
					}
					index++
				}
			})
		}
	}
}

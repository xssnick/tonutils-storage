package storage

import (
	"fmt"
	"os"
	"testing"

	"golang.org/x/sync/errgroup"
)

type testCaseAcquire struct {
	name  string
	paths []string
}

func TestAcquire(t *testing.T) {
	_FDLimit = 800
	tcs := genTestCasesAcquire()
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			defer cleanTmpFiles(tc.paths)
			err := testAcquire(tc.paths)
			if err != nil {
				t.Fatal(fmt.Sprintf("error acquiring error: %s", err.Error()))
			}
		})
	}
}

func BenchmarkAcquire(b *testing.B) {
	_FDLimit = 800
	paths := createTmpFiles(2000)
	defer cleanTmpFiles(paths)
	for i := 0; i < b.N; i++ {
		testAcquire(paths)
	}
}

func testAcquire(paths []string) error {
	fs := NewFSController()
	// acquire all the fd in tc.paths
	eg := new(errgroup.Group)
	for i, p := range paths {
		i, p := i, p
		eg.Go(func() error {
			fd, err := fs.Acquire(p)
			if i > _FDLimit-3 {
				fs.Free(fd)
			}

			return err
		})
	}

	return eg.Wait()
}

func genTestCasesAcquire() []testCaseAcquire {
	return []testCaseAcquire{
		{
			name:  "acquiring 3 file descriptors",
			paths: createTmpFiles(3),
		},
		{
			name:  "acquiring 797 file descriptors",
			paths: createTmpFiles(797),
		},
		{
			name:  "acquiring _FDLimit file descriptors",
			paths: createTmpFiles(_FDLimit),
		},
		{
			name:  "acquiring 2000 file descriptors",
			paths: createTmpFiles(2000),
		},
	}
}

func createTmpFiles(n int) []string {
	paths := make([]string, n)
	for i := 0; i < n; i++ {
		f, _ := os.CreateTemp("", "dump_")
		p := f.Name()
		f.Close()
		paths[i] = p
	}

	return paths
}

func cleanTmpFiles(paths []string) {
	// removing temporary files from system
	for _, p := range paths {
		os.Remove(p)
	}
}

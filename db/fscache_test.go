package db

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
	tcs := genTestCasesAcquire()
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			defer cleanTmpFiles(tc.paths)
			err := testAcquire(tc.paths)
			if err != nil {
				t.Fatalf("error acquiring error: %s", err.Error())
			}
		})
	}
}

func BenchmarkAcquire(b *testing.B) {
	paths := createTmpFiles(2000)
	defer cleanTmpFiles(paths)
	for i := 0; i < b.N; i++ {
		testAcquire(paths)
	}
}

func testAcquire(paths []string) error {
	fs := NewFSControllerCache(false)
	// acquire all the fd in tc.paths
	eg := new(errgroup.Group)
	for _, p := range paths {
		p := p
		eg.Go(func() error {
			_, err := fs.AcquireRead(p, make([]byte, 1), 0)
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
			paths: createTmpFiles(CachedFDLimit),
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
		f.WriteString(fmt.Sprint(i))
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

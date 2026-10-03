package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func smallCache(t *testing.T, limit int, paths int) (*FSControllerCache, []string) {
	t.Helper()
	previous := CachedFDLimit
	CachedFDLimit = limit
	t.Cleanup(func() { CachedFDLimit = previous })
	root := t.TempDir()
	cache := NewFSControllerCache(false)
	t.Cleanup(func() {
		if err := cache.RemoveDir(root); err != nil {
			t.Error(err)
		}
	})
	files := make([]string, paths)
	for i := range files {
		files[i] = filepath.Join(root, fmt.Sprint(i))
		if err := os.WriteFile(files[i], []byte{byte(i)}, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return cache, files
}

func TestAcquireReadConcurrentEviction(t *testing.T) {
	cache, paths := smallCache(t, 2, 8)
	var wg sync.WaitGroup
	for worker := 0; worker < 128; worker++ {
		wg.Go(func() {
			for i := 0; i < 100; i++ {
				id := (worker + i) % len(paths)
				buf := make([]byte, 1)
				if _, err := cache.AcquireRead(paths[id], buf, 0); err != nil {
					t.Errorf("read of immutable file failed: %v", err)
					return
				}
				if buf[0] != byte(id) {
					t.Errorf("read %d instead of %d", buf[0], id)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestAcquireReadEvictsIdleDescriptorWhenOldestBusy(t *testing.T) {
	cache, paths := smallCache(t, 2, 3)
	for _, path := range paths[:2] {
		if _, err := cache.AcquireRead(path, make([]byte, 1), 0); err != nil {
			t.Fatal(err)
		}
	}
	busy := cache.acquire(paths[0])
	busy.usedAt.Store(0)
	defer func() {
		busy.mx.RUnlock()
		cache.signalAvailable()
	}()
	done := make(chan error, 1)
	go func() {
		_, err := cache.AcquireRead(paths[2], make([]byte, 1), 0)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("idle descriptor was not evicted while the oldest was busy")
	}
}

func TestAcquireReadWaitDoesNotBlockCachedReads(t *testing.T) {
	cache, paths := smallCache(t, 1, 2)
	if _, err := cache.AcquireRead(paths[0], make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	busy := cache.acquire(paths[0])
	released := false
	defer func() {
		if !released {
			busy.mx.RUnlock()
			cache.signalAvailable()
		}
	}()
	miss := make(chan error, 1)
	go func() {
		_, err := cache.AcquireRead(paths[1], make([]byte, 1), 0)
		miss <- err
	}()
	hit := make(chan error, 1)
	go func() {
		_, err := cache.AcquireRead(paths[0], make([]byte, 1), 0)
		hit <- err
	}()
	select {
	case err := <-hit:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting for cache space blocked a cached read")
	}
	busy.mx.RUnlock()
	cache.signalAvailable()
	released = true
	select {
	case err := <-miss:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cache miss did not resume after a descriptor became idle")
	}
}

func TestResizePreservesPrefixAndRefreshesDescriptor(t *testing.T) {
	cache, paths := smallCache(t, 2, 1)
	path := paths[0]
	if _, err := cache.AcquireRead(path, make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("valid stale tail"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs := &OsFs{ctrl: cache}
	if err := fs.Resize(path, 5); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := cache.AcquireRead(path, buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "valid" {
		t.Fatalf("read stale descriptor: %q", buf)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 5 {
		t.Fatalf("resize failed: info=%v, err=%v", info, err)
	}
	if err := fs.Resize(path, 10); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) != 10 || string(data[:5]) != "valid" {
		t.Fatalf("growing a resume file lost its prefix: %q, %v", data, err)
	}
}

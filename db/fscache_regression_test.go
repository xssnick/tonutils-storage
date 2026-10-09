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

func TestAcquireReadEvictsIdleDescriptorWhenClockHandBusy(t *testing.T) {
	cache, paths := smallCache(t, 2, 3)
	for _, path := range paths[:2] {
		if _, err := cache.AcquireRead(path, make([]byte, 1), 0); err != nil {
			t.Fatal(err)
		}
	}
	busy := cache.acquire(paths[0])
	busy.referenced.Store(false)
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
		t.Fatal("idle descriptor was not evicted while the clock hand was busy")
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

func TestClockKeepsRecentlyReadDescriptor(t *testing.T) {
	cache, paths := smallCache(t, 3, 4)
	for _, path := range paths[:3] {
		if _, err := cache.AcquireRead(path, make([]byte, 1), 0); err != nil {
			t.Fatal(err)
		}
	}
	cache.mx.Lock()
	for _, desc := range cache.dsc {
		desc.referenced.Store(false)
	}
	cache.mx.Unlock()
	if _, err := cache.AcquireRead(paths[0], make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.AcquireRead(paths[3], make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	cache.mx.RLock()
	defer cache.mx.RUnlock()
	if len(cache.dsc) != 3 || cache.dsc[paths[0]] == nil || cache.dsc[paths[1]] != nil {
		t.Fatal("CLOCK did not give a recently read descriptor a second chance or exceeded the cap")
	}
}

func TestFileLifecycleDoesNotBlockUnrelatedCachedRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*FSControllerCache, string) error
	}{
		{"remove", func(cache *FSControllerCache, path string) error { return cache.RemoveFile(path) }},
		{"resize", func(cache *FSControllerCache, path string) error { return cache.ResizeFile(path, 1) }},
		{"remove directory", func(cache *FSControllerCache, path string) error { return cache.RemoveDir(filepath.Dir(path)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, paths := smallCache(t, 2, 2)
			// Keep the second descriptor outside the directory being removed.
			if tc.name == "remove directory" {
				paths[1] = filepath.Join(t.TempDir(), "other")
				if err := os.WriteFile(paths[1], []byte{1}, 0o600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = cache.RemoveDir(filepath.Dir(paths[1])) })
			}
			for _, path := range paths {
				if _, err := cache.AcquireRead(path, make([]byte, 1), 0); err != nil {
					t.Fatal(err)
				}
			}
			busy := cache.acquire(paths[0])
			released := false
			defer func() {
				if !released {
					busy.mx.RUnlock()
					cache.signalAvailable()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- tc.run(cache, paths[0]) }()
			// Wait until the operation detached its descriptor and is waiting
			// for the held read, without blocking on a faulty cache map lock.
			deadline := time.Now().Add(5 * time.Second)
			for {
				detached := false
				if cache.mx.TryRLock() {
					detached = cache.dsc[paths[0]] == nil
					cache.mx.RUnlock()
				}
				if detached {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("file operation did not release the map lock while waiting for a read")
				}
				time.Sleep(time.Millisecond)
			}
			hit := make(chan error, 1)
			go func() {
				_, err := cache.AcquireRead(paths[1], make([]byte, 1), 0)
				hit <- err
			}()
			select {
			case err := <-hit:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a file operation blocked an unrelated cached read")
			}
			busy.mx.RUnlock()
			cache.signalAvailable()
			released = true
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("file operation did not finish after its reader released")
			}
		})
	}
}

func TestAcquireReadConcurrentResize(t *testing.T) {
	cache, paths := smallCache(t, 2, 1)
	if err := os.WriteFile(paths[0], []byte("p"), 0o600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				buf := make([]byte, 1)
				if _, err := cache.AcquireRead(paths[0], buf, 0); err != nil {
					t.Errorf("read failed during resize: %v", err)
					return
				}
				if buf[0] != 'p' {
					t.Errorf("resize lost the existing prefix: %v", buf)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 100 {
			if err := cache.ResizeFile(paths[0], int64(1+i%2)); err != nil {
				t.Errorf("resize failed: %v", err)
				return
			}
		}
	})
	wg.Wait()
}

func TestCacheRecoversAfterFailedOpen(t *testing.T) {
	cache, paths := smallCache(t, 1, 3)
	if _, err := cache.AcquireRead(paths[0], make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(paths[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.AcquireRead(paths[2], make([]byte, 1), 0); !os.IsNotExist(err) {
		t.Fatalf("missing file returned %v", err)
	}
	if _, err := cache.AcquireRead(paths[1], make([]byte, 1), 0); err != nil {
		t.Fatalf("cache did not recover after failed open: %v", err)
	}
}

package db

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type FDescCache struct {
	file   *os.File
	usedAt atomic.Int64
	path   string
	mx     sync.RWMutex
}

// FSControllerCache caches file descriptors to avoid unnecessary open/close of most used files
type FSControllerCache struct {
	dsc      map[string]*FDescCache
	mx       sync.RWMutex
	noRemove bool
}

var CachedFDLimit = 800

func NewFSControllerCache(noRemove bool) *FSControllerCache {
	return &FSControllerCache{
		dsc:      map[string]*FDescCache{},
		noRemove: noRemove,
	}
}

func (f *FSControllerCache) AcquireRead(path string, p []byte, off int64) (n int, err error) {
	desc := f.acquire(path)

	if desc == nil {
		f.mx.Lock()
		desc = f.dsc[path]
		if desc == nil {
			if len(f.dsc) >= CachedFDLimit {
				for !f.clean() {
					// Retry till we clean something
					runtime.Gosched()
				}
			}

			fl, err := os.Open(path)
			if err != nil {
				f.mx.Unlock()
				return 0, err
			}

			desc = &FDescCache{
				file: fl,
				path: path,
			}
			desc.usedAt.Store(time.Now().UnixNano())
			desc.mx.RLock()

			f.dsc[path] = desc
			f.mx.Unlock()
		} else {
			f.mx.Unlock()
			desc.mx.RLock()
		}
		desc.usedAt.Store(time.Now().UnixNano())
	}
	defer desc.mx.RUnlock()

	return desc.file.ReadAt(p, off)
}

func (f *FSControllerCache) acquire(path string) *FDescCache {
	f.mx.RLock()
	desc, ok := f.dsc[path]
	if ok {
		desc.mx.RLock()
	}
	f.mx.RUnlock()
	if ok {
		desc.usedAt.Store(time.Now().UnixNano())
		return desc
	}
	return nil
}

// clean removes the oldest and currently unused file descriptor without sorting
func (f *FSControllerCache) clean() bool {
	var oldest *FDescCache

	// Find the oldest descriptor
	for _, desc := range f.dsc {
		if oldest == nil || desc.usedAt.Load() < oldest.usedAt.Load() {
			oldest = desc
		}
	}

	if oldest != nil && oldest.mx.TryLock() {
		defer oldest.mx.Unlock()
		_ = oldest.file.Close()
		delete(f.dsc, oldest.path)
		return true
	}
	return false
}

func (f *FSControllerCache) RemoveFile(path string) error {
	if f.noRemove {
		log.Println("attempt to remove file skipped because no-remove flag is set, file", path)
		return nil
	}

	f.mx.Lock()
	defer f.mx.Unlock()

	if desc := f.dsc[path]; desc != nil {
		desc.mx.Lock()
		_ = desc.file.Close()
		desc.mx.Unlock()
		delete(f.dsc, path)
	}

	path = filepath.Clean(path)

	remove := func() error {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}

	if runtime.GOOS == "windows" {
		const maxAttempts = 10
		const retryDelay = 100 * time.Millisecond

		var lastErr error
		for i := 0; i < maxAttempts; i++ {
			if err := remove(); err == nil {
				return nil
			} else {
				lastErr = err
			}

			if i == 0 {
				log.Println("first delete attempt failed, retrying sync, file", path)
			}
			if i < maxAttempts-1 {
				time.Sleep(retryDelay)
			}
		}

		log.Println("removal failed after retries, file", path, "err", lastErr)
		return lastErr
	}

	return remove()
}

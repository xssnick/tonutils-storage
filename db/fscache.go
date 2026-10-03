package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	dsc       map[string]*FDescCache
	mx        sync.RWMutex
	noRemove  bool
	available chan struct{}
}

var CachedFDLimit = 800

func NewFSControllerCache(noRemove bool) *FSControllerCache {
	return &FSControllerCache{
		dsc:       map[string]*FDescCache{},
		noRemove:  noRemove,
		available: make(chan struct{}, 1),
	}
}

func (f *FSControllerCache) AcquireRead(path string, p []byte, off int64) (n int, err error) {
	path = filepath.Clean(path)
	for {
		desc := f.acquire(path)
		if desc != nil {
			return f.read(desc, p, off)
		}
		f.mx.Lock()
		desc = f.dsc[path]
		if desc == nil {
			if CachedFDLimit <= 0 {
				f.mx.Unlock()
				fl, err := os.Open(path)
				if err != nil {
					return 0, err
				}
				defer fl.Close()
				return fl.ReadAt(p, off)
			}
			if len(f.dsc) >= CachedFDLimit && !f.clean() {
				f.mx.Unlock()
				// A completed read or removal makes another eviction possible.
				<-f.available
				continue
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
			desc.mx.RLock()
			f.mx.Unlock()
		}
		desc.usedAt.Store(time.Now().UnixNano())
		return f.read(desc, p, off)
	}
}

func (f *FSControllerCache) read(desc *FDescCache, p []byte, off int64) (n int, err error) {
	defer func() {
		desc.mx.RUnlock()
		f.signalAvailable()
	}()
	return desc.file.ReadAt(p, off)
}

func (f *FSControllerCache) signalAvailable() {
	select {
	case f.available <- struct{}{}:
	default:
	}
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

	// Keep the oldest idle descriptor locked until it has been removed.
	for _, desc := range f.dsc {
		if oldest != nil && desc.usedAt.Load() >= oldest.usedAt.Load() {
			continue
		}
		if !desc.mx.TryLock() {
			continue
		}
		if oldest != nil {
			oldest.mx.Unlock()
		}
		oldest = desc
	}

	if oldest != nil {
		defer oldest.mx.Unlock()
		_ = oldest.file.Close()
		delete(f.dsc, oldest.path)
		f.signalAvailable()
		return true
	}
	return false
}

func (f *FSControllerCache) RemoveFile(path string) error {
	if f.noRemove {
		log.Println("attempt to remove file skipped because no-remove flag is set, file", path)
		return nil
	}

	path = filepath.Clean(path)
	f.mx.Lock()
	defer f.mx.Unlock()
	f.invalidateLocked(path)
	defer f.signalAvailable()

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

func (f *FSControllerCache) invalidateLocked(path string) {
	if desc := f.dsc[path]; desc != nil {
		desc.mx.Lock()
		_ = desc.file.Close()
		delete(f.dsc, path)
		desc.mx.Unlock()
	}
}

// ResizeFile preserves already downloaded data and refreshes cached descriptors.
func (f *FSControllerCache) ResizeFile(path string, size int64) error {
	if size < 0 {
		return fmt.Errorf("negative file size")
	}
	path = filepath.Clean(path)
	f.mx.Lock()
	defer f.mx.Unlock()
	f.invalidateLocked(path)
	defer f.signalAvailable()
	return resizeFile(path, size)
}

// RemoveDir removes generated cache data, independently of noRemove for content.
func (f *FSControllerCache) RemoveDir(path string) error {
	path = filepath.Clean(path)
	f.mx.Lock()
	defer f.mx.Unlock()
	prefix := path + string(filepath.Separator)
	for name := range f.dsc {
		if name == path || strings.HasPrefix(name, prefix) {
			f.invalidateLocked(name)
		}
	}
	defer f.signalAvailable()
	return os.RemoveAll(path)
}

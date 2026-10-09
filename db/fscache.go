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
	file       *os.File
	referenced atomic.Bool
	path       string
	mx         sync.RWMutex
	prev, next *FDescCache
}

// FSControllerCache caches file descriptors to avoid unnecessary open/close of most used files
type FSControllerCache struct {
	dsc  map[string]*FDescCache
	hand *FDescCache
	mx   sync.RWMutex
	// Serialize descriptor creation and filesystem mutations without blocking cache hits.
	lifecycleMx sync.Mutex
	noRemove    bool
	available   chan struct{}
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
		f.lifecycleMx.Lock()
		if desc = f.acquire(path); desc != nil {
			f.lifecycleMx.Unlock()
			return f.read(desc, p, off)
		}
		if CachedFDLimit <= 0 {
			f.lifecycleMx.Unlock()
			fl, err := os.Open(path)
			if err != nil {
				return 0, err
			}
			defer fl.Close()
			return fl.ReadAt(p, off)
		}
		f.mx.Lock()
		var victim *FDescCache
		if len(f.dsc) >= CachedFDLimit {
			victim = f.evictLocked()
			if victim == nil {
				f.mx.Unlock()
				f.lifecycleMx.Unlock()
				// A completed read or removal makes another eviction possible.
				<-f.available
				continue
			}
		}
		f.mx.Unlock()
		if victim != nil {
			_ = victim.file.Close()
			victim.mx.Unlock()
		}
		fl, err := os.Open(path)
		if err != nil {
			f.lifecycleMx.Unlock()
			f.signalAvailable()
			return 0, err
		}
		desc = &FDescCache{file: fl, path: path}
		desc.referenced.Store(true)
		desc.mx.RLock()
		f.mx.Lock()
		f.insertLocked(desc)
		f.mx.Unlock()
		f.lifecycleMx.Unlock()
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
		desc.referenced.Store(true)
		return desc
	}
	return nil
}

func (f *FSControllerCache) insertLocked(desc *FDescCache) {
	if f.hand == nil {
		desc.prev, desc.next = desc, desc
		f.hand = desc
	} else {
		desc.prev, desc.next = f.hand.prev, f.hand
		f.hand.prev.next = desc
		f.hand.prev = desc
	}
	f.dsc[desc.path] = desc
}

func (f *FSControllerCache) detachLocked(desc *FDescCache) {
	if desc.next == desc {
		f.hand = nil
	} else {
		if f.hand == desc {
			f.hand = desc.next
		}
		desc.prev.next = desc.next
		desc.next.prev = desc.prev
	}
	delete(f.dsc, desc.path)
	desc.prev, desc.next = nil, nil
}

// CLOCK gives referenced descriptors a second chance. Each reference bit is
// cleared once before eviction, so idle-cache eviction is amortized constant time.
// The returned victim remains locked until its file has been closed.
func (f *FSControllerCache) evictLocked() *FDescCache {
	for remaining := 2 * len(f.dsc); remaining > 0; remaining-- {
		desc := f.hand
		f.hand = desc.next
		if !desc.mx.TryLock() {
			continue
		}
		if desc.referenced.Swap(false) {
			desc.mx.Unlock()
			continue
		}
		f.detachLocked(desc)
		return desc
	}
	return nil
}

func (f *FSControllerCache) RemoveFile(path string) error {
	if f.noRemove {
		log.Println("attempt to remove file skipped because no-remove flag is set, file", path)
		return nil
	}

	path = filepath.Clean(path)
	f.lifecycleMx.Lock()
	defer f.lifecycleMx.Unlock()
	f.invalidate(path)
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

func (f *FSControllerCache) invalidate(path string) {
	f.mx.Lock()
	desc := f.dsc[path]
	if desc != nil {
		f.detachLocked(desc)
	}
	f.mx.Unlock()
	if desc != nil {
		desc.mx.Lock()
		_ = desc.file.Close()
		desc.mx.Unlock()
	}
}

// ResizeFile preserves already downloaded data and refreshes cached descriptors.
func (f *FSControllerCache) ResizeFile(path string, size int64) error {
	if size < 0 {
		return fmt.Errorf("negative file size")
	}
	path = filepath.Clean(path)
	f.lifecycleMx.Lock()
	defer f.lifecycleMx.Unlock()
	f.invalidate(path)
	defer f.signalAvailable()
	return resizeFile(path, size)
}

// RemoveDir removes generated cache data, independently of noRemove for content.
func (f *FSControllerCache) RemoveDir(path string) error {
	path = filepath.Clean(path)
	f.lifecycleMx.Lock()
	defer f.lifecycleMx.Unlock()
	f.mx.Lock()
	prefix := path + string(filepath.Separator)
	var descriptors []*FDescCache
	for name, desc := range f.dsc {
		if name == path || strings.HasPrefix(name, prefix) {
			f.detachLocked(desc)
			descriptors = append(descriptors, desc)
		}
	}
	f.mx.Unlock()
	for _, desc := range descriptors {
		desc.mx.Lock()
		_ = desc.file.Close()
		desc.mx.Unlock()
	}
	defer f.signalAvailable()
	return os.RemoveAll(path)
}

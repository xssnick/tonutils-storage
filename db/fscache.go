package db

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type FDescCache struct {
	file   *os.File
	usedAt time.Time
	path   string
	mx     sync.Mutex
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

			desc = &FDescCache{
				path:   path,
				usedAt: time.Now(),
			}
			desc.mx.Lock()

			f.dsc[path] = desc
			f.mx.Unlock()

			fl, err := os.Open(path)
			if err != nil {
				f.mx.Lock()
				// unlikely, rare case
				delete(f.dsc, path)
				f.mx.Unlock()

				desc.mx.Unlock()
				return 0, err
			}
			desc.file = fl
		} else {
			f.mx.Unlock()
			desc.mx.Lock()
		}
		desc.usedAt = time.Now()
	}
	defer desc.mx.Unlock()

	return desc.file.ReadAt(p, off)
}

func (f *FSControllerCache) acquire(path string) *FDescCache {
	f.mx.RLock()
	desc, ok := f.dsc[path]
	f.mx.RUnlock()
	if ok {
		desc.mx.Lock()
		desc.usedAt = time.Now()
		return desc
	}
	return nil
}

// clean removes the oldest and currently unused file descriptor without sorting
func (f *FSControllerCache) clean() bool {
	var oldest *FDescCache

	// Find the oldest descriptor
	for _, desc := range f.dsc {
		if oldest == nil || desc.usedAt.Before(oldest.usedAt) {
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

func (f *FSControllerCache) RemoveFile(path string) (err error) {
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
		if err = remove(); err == nil {
			return
		}

		log.Println("first delete attempt failed, retrying async in 50ms, file", path)
		go func() {
			// windows can still hold a file for some time, so we retry
			for i := 0; i < 4; i++ {
				if err = remove(); err == nil {
					log.Println("removed asynchronously", path)
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			log.Println("async removal failed, file", path)
		}()

		return nil
	}

	return remove()
}

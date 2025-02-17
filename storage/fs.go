package storage

import (
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type FDesc struct {
	file   *os.File
	usedAt time.Time
	path   string

	mx sync.Mutex
}

// FSController caches files descriptors to avoid unnecessary open/close of most used files
type FSController struct {
	dsc map[string]*FDesc
	mx  sync.RWMutex
}

func NewFSController() *FSController {
	return &FSController{
		dsc: map[string]*FDesc{},
	}
}

const _FDLimit = 800

func (f *FSController) Acquire(path string) (*FDesc, error) {
	desc := f.acquire(path)

	if desc == nil {
		f.mx.Lock()
		desc = f.dsc[path]
		if desc == nil {
			if len(f.dsc) >= _FDLimit {
				for !f.clean() {
					// retry till we clean something
					runtime.Gosched()
				}
			}

			fl, err := os.Open(path)
			if err != nil {
				f.mx.Unlock()
				return nil, err
			}

			desc = &FDesc{
				path:   path,
				file:   fl,
				usedAt: time.Now(),
			}
			f.dsc[path] = desc
		}
		f.mx.Unlock()

		desc.mx.Lock()
		desc.usedAt = time.Now()
	}
	return desc, nil
}

func (f *FDesc) Free() {
	f.mx.Unlock()
}

func (f *FDesc) Get() io.ReaderAt {
	return f.file
}

func (f *FSController) acquire(path string) *FDesc {
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

// clean the oldest and currently not used file
func (f *FSController) clean() bool {
	list := make([]*FDesc, 0, len(f.dsc))
	for _, desc := range f.dsc {
		list = append(list, desc)
	}

	now := time.Now()
	sort.Slice(list, func(i, j int) bool {
		return now.Sub(list[i].usedAt) > now.Sub(list[j].usedAt)
	})

	for _, desc := range list {
		if desc.mx.TryLock() {
			// it is not used now, so we can close it
			_ = desc.file.Close()
			delete(f.dsc, desc.path)
			desc.mx.Unlock()
			return true
		}
	}
	return false
}

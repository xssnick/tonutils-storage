package storage

import (
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

const _FDLimit = math.MaxInt32

type FDesc struct {
	file *os.File
	path string
	mx   sync.Mutex
}

func NewFDesc(f *os.File, path string) *FDesc {
	return &FDesc{
		file: f,
		path: path,
	}
}

func (f *FDesc) Get() io.ReaderAt {
	return f.file
}

// FSController caches files descriptors to avoid unnecessary open/close of most used files
type FSController struct {
	dsc     map[string]*FDesc
	counter atomic.Int32
	// keep track of the least recently availables file descriptors
	// they are appended as soon as they are freed, resulting in
	// oldest file descriptors available to newest file descriptors available
	availableFdsToClose []*FDesc
	// limitReached is a conditional variable
	limitReached sync.Cond

	mx sync.Mutex
}

func NewFSController() *FSController {
	fs := new(FSController)
	fs.dsc = make(map[string]*FDesc)
	fs.availableFdsToClose = make([]*FDesc, 0)
	fs.limitReached.L = &fs.mx

	return fs
}

// Acquire given a path of a file, returns the FDesc associated
// with that path
func (fs *FSController) Acquire(path string) (*FDesc, error) {
	fs.mx.Lock()
	defer fs.mx.Unlock()

	fd, ok := fs.dsc[path]
	if !ok {
		for fs.counter.Load() >= _FDLimit && len(fs.availableFdsToClose) == 0 {
			fs.limitReached.Wait()
		}

		// remove one fd from list of available fd to be closed
		fs.clean()

		// open file
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		// increase counter of files in used
		fs.counter.Add(1)
		// we need to check position
		fd := NewFDesc(f, path)
		fs.dsc[path] = fd
		fd.mx.Lock()

		return fd, nil
	}

	fd.mx.Lock()

	return fd, nil
}

// Free unlocks a file descriptor given its path
func (fs *FSController) Free(fd *FDesc) {
	fs.mx.Lock()
	defer fs.mx.Unlock()

	fd.mx.Unlock()
	// add to most recently available file descriptors
	fs.availableFdsToClose = append(fs.availableFdsToClose, fd)
	if fs.counter.Load() >= _FDLimit {
		fs.limitReached.Signal()
	}
}

// Get a file descriptor reader given its path
func (fs *FSController) Get(path string) io.ReaderAt {
	fd, ok := fs.dsc[path]
	if !ok {
		return nil
	}

	return fd.file
}

// Release given its path release a file descriptor removing it from cache
// and decreasing the counter of it
func (fs *FSController) Release(path string) error {
	fs.mx.Lock()
	defer fs.mx.Unlock()

	dsc, ok := fs.dsc[path]
	if !ok {
		return nil
	}

	dsc.mx.Unlock()
	fs.counter.Add(-1)

	delete(fs.dsc, path)
	fs.limitReached.Signal()

	return nil
}

// clean returns true if there's any "available" file descriptor that could be released
// closing it and removing it from cache. False otherwise.
// clean is called inside acquire which already called Lock
func (fs *FSController) clean() {
	if len(fs.availableFdsToClose) == 0 {
		return
	}

	// retrieve oldest available file descriptor
	// which mutex was already unlocked
	fd := fs.availableFdsToClose[0]
	_ = fd.file.Close()
	// remove from availables Fds
	fs.availableFdsToClose = fs.availableFdsToClose[1:]

	// decrease counter of used fds
	fs.counter.Add(-1)
	delete(fs.dsc, fd.path)
}

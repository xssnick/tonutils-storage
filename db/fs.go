package db

import (
	"fmt"
	"github.com/xssnick/tonutils-storage/storage"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type OsFs struct{}

func (o *OsFs) Open(name string, mode storage.OpenMode) (storage.FSFile, error) {
	if mode == storage.OpenModeWrite {
		if err := os.MkdirAll(filepath.Dir(name), os.ModePerm); err != nil {
			return nil, err
		}

		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open/create file %s: %w", name, err)
		}
		return f, nil
	}
	panic("unsupported mode")
}

func (o *OsFs) Exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (s *Storage) GetFS() storage.FS {
	return &s.fs
}

type fileInfo struct {
	name string
	size uint64
	path string
}

func (f fileInfo) GetName() string {
	return f.name
}

func (f fileInfo) GetSize() uint64 {
	return f.size
}

func (f fileInfo) CreateReader() (io.ReadCloser, error) {
	fl, err := os.Open(f.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", f.path, err)
	}
	return fl, nil
}

func (s *Storage) DetectFileRefs(path string) (rootPath string, dirName string, _ []storage.FileRef, _ error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", "", nil, err
	}

	fi, err := os.Stat(path)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to stat file %s: %w", path, err)
	}

	if fi.IsDir() {
		files, err := s.GetAllFilesRefsInDir(path)
		if err != nil {
			return "", "", nil, err
		}

		dir := filepath.Base(path) + "/"
		if strings.HasPrefix(dir, ".") || strings.HasPrefix(dir, "/") {
			// fallback to empty name
			dir = ""
		}

		return filepath.Dir(path), dir, files, nil
	}

	file, err := s.GetSingleFileRef(path)
	if err != nil {
		return "", "", nil, err
	}

	return filepath.Dir(path), "", []storage.FileRef{file}, nil
}

func (s *Storage) GetSingleFileRef(path string) (storage.FileRef, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	// stat is not always gives the right file size, so we open file and find the end
	fl, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer fl.Close()

	sz, err := fl.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek file end %s: %w", path, err)
	}

	return fileInfo{
		name: filepath.Base(path),
		size: uint64(sz),
		path: path,
	}, nil
}

func (s *Storage) GetAllFilesRefsInDir(path string) ([]storage.FileRef, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	var files []storage.FileRef
	err = filepath.Walk(path, func(filePath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		name := filePath[len(path)+1:]
		name = strings.ReplaceAll(name, "\\", "/") // to unix style

		// stat is not always gives the right file size, so we open file and find the end
		fl, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filePath, err)
		}

		sz, err := fl.Seek(0, io.SeekEnd)
		fl.Close()
		if err != nil {
			return fmt.Errorf("failed to seek file end %s: %w", filePath, err)
		}

		files = append(files, fileInfo{
			name: name,
			size: uint64(sz),
			path: filePath,
		})
		return nil
	})
	if err != nil {
		err = fmt.Errorf("failed to scan directory '%s': %w", path, err)
		return nil, err
	}
	return files, nil
}

package db

import (
	"fmt"
	"github.com/xssnick/tonutils-storage/storage"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Node struct {
	Id       string
	Children []*Node
}

func buildTreeFromDir(baseDir string) *Node {
	_, err := os.ReadDir(baseDir)
	if err != nil {
		return nil
	}
	root := &Node{
		Id: baseDir,
	}
	queue := make(chan *Node, 100)
	queue <- root
	for {
		if len(queue) == 0 {
			break
		}
		data, ok := <-queue
		if ok {
			// Iterate all the contents in the dir
			curDir := (*data).Id
			isDir, err := ifDir(curDir)
			if err != nil {
				continue
			}
			if isDir {
				contents, _ := os.ReadDir(curDir)

				data.Children = make([]*Node, len(contents))
				for i, content := range contents {
					node := new(Node)
					node.Id = filepath.Join(curDir, content.Name())
					data.Children[i] = node
					if content.IsDir() {
						queue <- node
					}
				}
			}
		}
	}
	return root
}

func ifDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func recursiveEmptyDelete(root *Node, fs storage.FSController) error {
	// If the current root is not pointing to any dir
	if root == nil {
		return nil
	}

	var firstErr error
	for _, each := range root.Children {
		if err := recursiveEmptyDelete(each, fs); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	isDir, err := ifDir(root.Id)
	if err != nil {
		if os.IsNotExist(err) {
			return firstErr
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to stat %s: %w", root.Id, err)
		}
		return firstErr
	}
	if !isDir {
		return firstErr
	}

	content, err := os.ReadDir(root.Id)
	if err != nil {
		if os.IsNotExist(err) {
			return firstErr
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to read dir %s: %w", root.Id, err)
		}
		return firstErr
	}

	for _, entry := range content {
		if entry.IsDir() || !isIgnoredSystemFile(entry.Name()) {
			continue
		}

		path := filepath.Join(root.Id, entry.Name())
		if err := fs.RemoveFile(path); err != nil && !os.IsNotExist(err) {
			log.Println("failed to remove ignored system file", path, err.Error())
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	content, err = os.ReadDir(root.Id)
	if err != nil {
		if os.IsNotExist(err) {
			return firstErr
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to read dir %s after cleanup: %w", root.Id, err)
		}
		return firstErr
	}
	if len(content) != 0 {
		names := make([]string, 0, len(content))
		for _, entry := range content {
			names = append(names, entry.Name())
		}
		log.Println("skip remove of", root.Id, "contains unknown files:", strings.Join(names, ", "))
		if firstErr == nil {
			firstErr = fmt.Errorf("%s contains unknown files", root.Id)
		}
		return firstErr
	}

	if err := fs.RemoveFile(root.Id); err != nil && !os.IsNotExist(err) {
		log.Println("failed to remove", root.Id, err.Error())
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

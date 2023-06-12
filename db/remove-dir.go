package db

import (
	"os"
	"path/filepath"
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
			if ifDir(curDir) {
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

func ifDir(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if info.IsDir() {
		return true
	}
	return false
}

func recursiveEmptyDelete(root *Node) {
	// If the current root is not pointing to any dir
	if root == nil {
		return
	}
	for _, each := range root.Children {
		recursiveEmptyDelete(each)
	}
	if !ifDir(root.Id) {
		return
	} else if content, _ := os.ReadDir(root.Id); len(content) != 0 {
		return
	}
	os.Remove(root.Id)
}

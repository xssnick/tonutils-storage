.PHONY: all build

ver := $(shell git describe --tags --always --dirty)

build:
	go build -ldflags "-w -s -X main.GitCommit=$(ver)" -o build/tonutils-storage cli/main.go

all:
	GOOS=linux GOARCH=amd64 go build -ldflags "-w -s -X main.GitCommit=$(ver)" -o build/tonutils-storage-linux-amd64 cli/main.go
	GOOS=linux GOARCH=arm64 go build -ldflags "-w -s -X main.GitCommit=$(ver)" -o build/tonutils-storage-linux-arm64 cli/main.go
	GOOS=darwin GOARCH=arm64 go build -ldflags "-w -s -X main.GitCommit=$(ver)" -o build/tonutils-storage-mac-arm64 cli/main.go
	GOOS=darwin GOARCH=amd64 go build -ldflags "-w -s -X main.GitCommit=$(ver)" -o build/tonutils-storage-mac-amd64 cli/main.go
	GOOS=windows GOARCH=amd64 go build -ldflags "-w -s -X main.GitCommit=$(ver)" -o build/tonutils-storage-x64.exe cli/main.go
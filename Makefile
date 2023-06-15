ver := $(shell git log -1 --pretty=format:%h)

compile:
	GOOS=linux GOARCH=amd64 go build -ldflags "-X main.GitCommit=$(ver)" -o build/tonutils-storage-linux-amd64 cli/main.go
	GOOS=linux GOARCH=arm64 go build -ldflags "-X main.GitCommit=$(ver)" -o build/tonutils-storage-linux-arm64 cli/main.go
	GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.GitCommit=$(ver)" -o build/tonutils-storage-mac-arm64 cli/main.go
	GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.GitCommit=$(ver)" -o build/tonutils-storage-mac-amd64 cli/main.go
	GOOS=windows GOARCH=amd64 go build -ldflags "-X main.GitCommit=$(ver)" -o build/tonutils-storage-x64.exe cli/main.go
package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
)

type memoryFileRef struct {
	name string
	data []byte
	size uint64
}

func (f memoryFileRef) GetName() string { return f.name }
func (f memoryFileRef) GetSize() uint64 { return f.size }
func (f memoryFileRef) CreateReader() (io.ReaderAt, func() error, error) {
	return bytes.NewReader(f.data), func() error { return nil }, nil
}

type pieceFailureStore struct {
	*e2eStorage
	err error
}

func (s *pieceFailureStore) SetPiece([]byte, uint32, *PieceInfo) error { return s.err }

func TestCreateTorrentReturnsLastPieceWriteFailure(t *testing.T) {
	wantErr := errors.New("disk write failed")
	store := &pieceFailureStore{e2eStorage: newE2EStorage(4096), err: wantErr}
	torrent, err := CreateTorrentWithInitialHeader(t.Context(), t.TempDir(), "", &TorrentHeader{}, store, nil,
		[]FileRef{memoryFileRef{name: "payload", data: []byte{1}, size: 1}}, nil, false)
	if torrent != nil || !errors.Is(err, wantErr) {
		t.Fatalf("creation must report failed persistence: torrent=%v err=%v", torrent, err)
	}
}

func TestCreateTorrentRejectsChangedSourceSizes(t *testing.T) {
	for _, test := range []struct {
		name     string
		actual   int
		declared uint64
	}{
		{name: "grew", actual: 8192, declared: 1},
		{name: "shrank", actual: 1, declared: 8192},
		{name: "shrank_in_parallel_range", actual: 8192, declared: 32768},
		{name: "empty_became_nonempty", actual: 1, declared: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := CreateTorrentWithInitialHeader(t.Context(), t.TempDir(), "", &TorrentHeader{}, newE2EStorage(4096), nil,
				[]FileRef{memoryFileRef{name: "payload", data: make([]byte, test.actual), size: test.declared}}, nil, false)
			if err == nil || !strings.Contains(err.Error(), "source size changed") {
				t.Fatalf("expected source size error, got %v", err)
			}
		})
	}
}

func TestCreateTorrentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := CreateTorrentWithInitialHeader(ctx, t.TempDir(), "", &TorrentHeader{}, newE2EStorage(4096), nil,
		[]FileRef{memoryFileRef{name: "payload", data: []byte{1}, size: 1}}, nil, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestCreateTorrentRejectsInvalidInitialHeader(t *testing.T) {
	for _, header := range []*TorrentHeader{
		{DirNameSize: 257, DirName: bytes.Repeat([]byte("x"), 257)},
		{TotalDataSize: 1},
		{DirNameSize: 1},
	} {
		if _, err := CreateTorrentWithInitialHeader(t.Context(), t.TempDir(), "", header, newE2EStorage(512), nil,
			[]FileRef{memoryFileRef{name: "empty"}}, nil, false); err == nil {
			t.Fatal("creation accepted a header that cannot be consumed")
		}
	}
}

func TestTorrentRejectsDuplicateFilePaths(t *testing.T) {
	for _, names := range [][]string{{"x", "x"}, {"a//b", "a/b"}, {"a", "a/b"}, {"a/b/c", "a"}} {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			header := &TorrentHeader{FilesCount: 2, DataIndex: []uint64{1, 2}}
			for _, name := range names {
				header.Names = append(header.Names, name...)
				header.TotalNameSize += uint64(len(name))
				header.NameIndex = append(header.NameIndex, header.TotalNameSize)
			}
			serialized, err := tl.Serialize(header, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tl.Parse(&TorrentHeader{}, serialized, true); err == nil {
				t.Fatal("duplicate paths must be rejected during parsing")
			}
			torrent := &Torrent{Header: header, Info: &TorrentInfo{PieceSize: 4096, HeaderSize: uint64(len(serialized)), FileSize: uint64(len(serialized)) + 2}}
			if _, err = torrent.ListFiles(); err == nil {
				t.Fatal("duplicate paths must be rejected by ListFiles")
			}
			_, err = CreateTorrentWithInitialHeader(t.Context(), t.TempDir(), "", &TorrentHeader{}, newE2EStorage(4096), nil,
				[]FileRef{memoryFileRef{name: names[0]}, memoryFileRef{name: names[1]}}, nil, false)
			if err == nil {
				t.Fatal("duplicate paths must be rejected during creation")
			}
		})
	}
}

func TestTorrentReadsLogicalFileBounds(t *testing.T) {
	root := t.TempDir()
	store := newE2EStorage(512)
	files := []FileRef{memoryFileRef{name: "a", data: []byte("first"), size: 5}, memoryFileRef{name: "b", data: []byte("other"), size: 5}}
	torrent, err := CreateTorrentWithInitialHeader(t.Context(), root, "", &TorrentHeader{}, store, nil, files, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		f, err := store.fs.Open(root+"/"+file.GetName(), OpenModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		data := file.(memoryFileRef).data
		if file.GetName() == "a" {
			data = append(append([]byte(nil), data...), []byte("stale tail")...)
		}
		_, err = f.WriteAt(data, 0)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = torrent.GetPieceProof(0); err != nil {
		t.Fatalf("trailing physical bytes must not affect piece reconstruction: %v", err)
	}
}

func TestVerificationPreservesLocallyCreatedSource(t *testing.T) {
	root := t.TempDir()
	store := newE2EStorage(512)
	torrent, err := CreateTorrentWithInitialHeader(t.Context(), root, "", &TorrentHeader{}, store, nil,
		[]FileRef{memoryFileRef{name: "payload", data: []byte("original"), size: 8}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	file, err := store.fs.Open(root+"/payload", OpenModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	changed := []byte("modified with additional data")
	if _, err = file.WriteAt(changed, 0); err != nil {
		t.Fatal(err)
	}
	file.Close()
	torrent.globalCtx, torrent.pause = context.WithCancel(t.Context())
	defer torrent.pause()
	intact, err := torrent.Verify(t.Context(), true)
	if err != nil || intact {
		t.Fatalf("changed source must fail verification: intact=%v err=%v", intact, err)
	}
	torrent.Wait()
	got := make([]byte, len(changed))
	n, err := store.fs.GetController().AcquireRead(root+"/payload", got, 0)
	if err != nil || n != len(got) || !bytes.Equal(got, changed) {
		t.Fatalf("verification changed the local source: data=%q n=%d err=%v", got, n, err)
	}
	if torrent.globalCtx.Err() != nil {
		t.Fatal("verification started a downloader for the local source")
	}
}

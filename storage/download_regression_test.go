package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type delayedVerificationFS struct{ reads atomic.Int32 }

func (*delayedVerificationFS) Open(string, OpenMode) (FSFile, error) {
	return nil, errors.New("unexpected write")
}
func (*delayedVerificationFS) Exists(name string) bool {
	return !strings.Contains(name, ".tonutils-storage-pieces")
}
func (*delayedVerificationFS) Delete(string) error           { return os.ErrNotExist }
func (f *delayedVerificationFS) GetController() FSController { return f }
func (*delayedVerificationFS) RemoveFile(string) error       { return nil }
func (f *delayedVerificationFS) AcquireRead(_ string, data []byte, off int64) (int, error) {
	f.reads.Add(1)
	if off == 0 {
		time.Sleep(200 * time.Millisecond)
		return 0, errors.New("corrupted first piece")
	}
	clear(data)
	return len(data), nil
}

type delayedVerificationStore struct {
	*e2eStorage
	fs *delayedVerificationFS
}

func (s *delayedVerificationStore) GetFS() FS { return s.fs }

func TestVerifyWaitsForOutOfOrderCorruption(t *testing.T) {
	const pieceSize = 1 << 20
	const count = 257
	hash := sha256.Sum256(make([]byte, pieceSize))
	hashes := make([]merkleHash, count)
	for i := range hashes {
		hashes[i] = hash
	}
	root, err := buildMerkleTree(context.Background(), hashes, 0)
	if err != nil {
		t.Fatal(err)
	}
	store := &delayedVerificationStore{e2eStorage: newE2EStorage(pieceSize), fs: &delayedVerificationFS{}}
	tor := NewTorrent("unused", store, nil)
	tor.BagID = make([]byte, 32)
	tor.CreatedLocally = true
	tor.downloadAll = true
	tor.Info = &TorrentInfo{PieceSize: pieceSize, FileSize: count * pieceSize, RootHash: root.Hash(0)}
	tor.Header = &TorrentHeader{FilesCount: 1, TotalNameSize: 4, DataIndex: []uint64{count * pieceSize}, NameIndex: []uint64{4}, Names: []byte("file")}
	for id := uint32(0); id < count; id++ {
		proof, err := tor.fastProof(root, id, count)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.SetPiece(tor.BagID, id, &PieceInfo{Proof: proof.ToBOCWithFlags(false)}); err != nil {
			t.Fatal(err)
		}
	}
	tor.InitMask()
	intact, err := tor.Verify(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if intact {
		t.Fatal("verification missed delayed corrupt piece")
	}
	reads := store.fs.reads.Load()
	time.Sleep(10 * time.Millisecond)
	if store.fs.reads.Load() != reads {
		t.Fatal("verification workers still running after return")
	}
}

func setupLoopbackFiles(t *testing.T, files map[string][]byte, selected []uint32, ordered bool) (*Torrent, *Torrent) {
	t.Helper()
	seedDir, downloadDir := t.TempDir(), t.TempDir()
	seedStore, downStore := newE2EStorage(4096), newE2EStorage(4096)
	_, seedKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, downKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	seedServer := &loopbackTorrentServer{id: seedKey.Public().(ed25519.PublicKey), key: seedKey}
	downServer := &loopbackTorrentServer{id: downKey.Public().(ed25519.PublicKey), key: downKey}
	var names []string
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var refs []FileRef
	for _, name := range names {
		path := filepath.Join(seedDir, name)
		if err = os.WriteFile(path, files[name], 0o644); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, e2eFileRef{path: path, name: name, size: uint64(len(files[name]))})
	}
	seed, err := CreateTorrentWithInitialHeader(context.Background(), seedDir, "download regression", &TorrentHeader{}, seedStore, NewConnector(seedServer), refs, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	activateTorrent(seed, true)
	seed.downloadAll = true
	if err = seedStore.SetTorrent(seed); err != nil {
		t.Fatal(err)
	}
	down := NewTorrent(downloadDir, downStore, NewConnector(downServer))
	down.BagID = append([]byte(nil), seed.BagID...)
	down.downloadAll = selected == nil
	down.activeFiles = append([]uint32(nil), selected...)
	down.downloadOrdered = ordered
	activateTorrent(down, true)
	if err = downStore.SetTorrent(down); err != nil {
		t.Fatal(err)
	}
	seedSide, downSide := &loopbackSide{torrent: seed}, &loopbackSide{torrent: down}
	seedSide.conn = newLoopbackPeerConnection(seedServer.id, downServer.id, downKey.Public().(ed25519.PublicKey), downSide, "downloader")
	downSide.conn = newLoopbackPeerConnection(downServer.id, seedServer.id, seedKey.Public().(ed25519.PublicKey), seedSide, "seeder")
	peer, attempt := down.prepareStoragePeer(seed.BagID, nil, downSide.conn, nil)
	if attempt == nil {
		t.Fatal("missing session attempt")
	}
	if err = peer.initializeSession(attempt, true); err != nil {
		t.Fatal(err)
	}
	peer.touch()
	t.Cleanup(func() { down.Stop(); seed.Stop(); down.Wait(); seed.Wait() })
	return seed, down
}

func completeTestDownload(t *testing.T, torrent *Torrent) {
	t.Helper()
	done, errs := make(chan struct{}, 1), make(chan error, 1)
	if err := torrent.startDownload(func(event Event) {
		switch event.Name {
		case EventDone:
			done <- struct{}{}
		case EventErr:
			select {
			case errs <- event.Value.(error):
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("download timeout")
	}
	torrent.Wait()
}

func TestLoopbackEmptyFiles(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, mixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("ordered_%v_mixed_%v", ordered, mixed), func(t *testing.T) {
				files := map[string][]byte{"empty": {}}
				if mixed {
					files["payload"] = []byte("payload")
				}
				_, down := setupLoopbackFiles(t, files, nil, ordered)
				completeTestDownload(t, down)
				for name, want := range files {
					got, err := os.ReadFile(filepath.Join(down.Path, name))
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("file %s content mismatch", name)
					}
				}
				if !down.IsCompleted() {
					t.Fatal("download did not commit all pieces")
				}
			})
		}
	}
}

type startupVerifyingStore struct{ *e2eStorage }

func (*startupVerifyingStore) VerifyOnStartup() bool { return true }

func TestLoopbackRestartRestoresDeletedEmptyFile(t *testing.T) {
	for _, startupVerify := range []bool{false, true} {
		t.Run(fmt.Sprint(startupVerify), func(t *testing.T) {
			_, down := setupLoopbackFiles(t, map[string][]byte{"empty": {}}, nil, false)
			completeTestDownload(t, down)
			down.Stop()
			down.Wait()
			if startupVerify {
				down.db = &startupVerifyingStore{e2eStorage: down.db.(*e2eStorage)}
			}
			if err := os.Remove(filepath.Join(down.Path, "empty")); err != nil {
				t.Fatal(err)
			}
			if err := down.Start(true, true, false); err != nil {
				t.Fatal(err)
			}
			waitForTestCondition(t, 2*time.Second, func() bool {
				_, err := os.Stat(filepath.Join(down.Path, "empty"))
				return err == nil
			}, "recreated empty file")
			down.Stop()
			down.Wait()
			if !down.IsCompleted() {
				t.Fatal("empty file verification removed an unrelated piece")
			}
		})
	}
}

type blockedDownloadFS struct {
	FS
	started chan struct{}
	release chan struct{}
	once    sync.Once
	reads   atomic.Int32
}

func (f *blockedDownloadFS) Open(name string, mode OpenMode) (FSFile, error) {
	file, err := f.FS.Open(name, mode)
	if err != nil {
		return nil, err
	}
	return &blockedDownloadFile{FSFile: file, fs: f}, nil
}
func (f *blockedDownloadFS) Resize(name string, size int64) error { return os.Truncate(name, size) }
func (f *blockedDownloadFS) GetController() FSController          { return f }
func (f *blockedDownloadFS) RemoveFile(name string) error {
	return f.FS.GetController().RemoveFile(name)
}
func (f *blockedDownloadFS) AcquireRead(name string, data []byte, off int64) (int, error) {
	f.reads.Add(1)
	return f.FS.GetController().AcquireRead(name, data, off)
}

type blockedDownloadFile struct {
	FSFile
	fs *blockedDownloadFS
}

func (f *blockedDownloadFile) WriteAt(data []byte, off int64) (int, error) {
	f.fs.once.Do(func() { close(f.fs.started); <-f.fs.release })
	return f.FSFile.WriteAt(data, off)
}

type blockedDownloadStore struct {
	*e2eStorage
	fs *blockedDownloadFS
}

func (s *blockedDownloadStore) GetFS() FS { return s.fs }

func TestVerifyWaitsForDownloadWrite(t *testing.T) {
	_, down := setupLoopbackFiles(t, map[string][]byte{"payload": []byte("expected")}, nil, false)
	baseStore := down.db.(*e2eStorage)
	fileSystem := &blockedDownloadFS{FS: baseStore.GetFS(), started: make(chan struct{}), release: make(chan struct{})}
	down.db = &blockedDownloadStore{e2eStorage: baseStore, fs: fileSystem}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fileSystem.release) }) }
	defer release()
	if err := down.startDownload(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fileSystem.started:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not start")
	}
	verified := make(chan error, 1)
	go func() { _, err := down.Verify(context.Background(), true); verified <- err }()
	waitForTestCondition(t, time.Second, func() bool {
		down.mx.RLock()
		defer down.mx.RUnlock()
		return down.isVerificationInProgress
	}, "verification waiting for writer")
	if fileSystem.reads.Load() != 0 {
		t.Fatal("verification read a destination while its writer was active")
	}
	select {
	case err := <-verified:
		t.Fatalf("verification returned before writer finished: %v", err)
	default:
	}
	release()
	select {
	case err := <-verified:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("verification did not resume")
	}
	down.Wait()
	if _, err := down.getPieceInternal(0, true); err != nil {
		t.Fatal(err)
	}
}

func TestLoopbackSelectedBoundaryPieceSurvivesRestart(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprint(ordered), func(t *testing.T) {
			files := map[string][]byte{"a": bytes.Repeat([]byte("a"), 100), "b": bytes.Repeat([]byte("b"), 100)}
			seed, down := setupLoopbackFiles(t, files, []uint32{1}, ordered)
			completeTestDownload(t, down)
			if _, err := os.Stat(filepath.Join(down.Path, "a")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unselected file was created")
			}
			info, err := down.GetFileOffsetsByID(1)
			if err != nil {
				t.Fatal(err)
			}
			if !bitsetHas(down.PiecesMask(), info.FromPiece) {
				t.Fatal("shared piece not available")
			}
			want, err := seed.getPieceInternal(info.FromPiece, true)
			if err != nil {
				t.Fatal(err)
			}
			store := down.db
			restarted := NewTorrent(down.Path, store, nil)
			restarted.BagID, restarted.Info, restarted.Header = down.BagID, down.Info, down.Header
			restarted.activeFiles = []uint32{1}
			restarted.InitMask()
			got, err := restarted.getPieceInternal(info.FromPiece, true)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Data, want.Data) {
				t.Fatal("restarted seed returned incomplete boundary piece")
			}
			if err = os.WriteFile(filepath.Join(down.Path, "b"), bytes.Repeat([]byte("x"), 100), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err = restarted.getPieceInternal(info.FromPiece, true); err == nil {
				t.Fatal("cache hid corrupt selected destination")
			}
		})
	}
}

func TestLoopbackDownloadTrimsOversizedResumeFile(t *testing.T) {
	_, down := setupLoopbackFiles(t, map[string][]byte{"payload": []byte("expected")}, nil, false)
	if err := os.WriteFile(filepath.Join(down.Path, "payload"), bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	completeTestDownload(t, down)
	got, err := os.ReadFile(filepath.Join(down.Path, "payload"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "expected" {
		t.Fatalf("resume file was not resized: %d bytes", len(got))
	}
}

func TestLoopbackSelectionExpansionRestoresCachedBytes(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, stale := range []bool{false, true} {
			t.Run(fmt.Sprintf("ordered_%v_stale_%v", ordered, stale), func(t *testing.T) {
				files := map[string][]byte{"a": bytes.Repeat([]byte("a"), 100), "b": bytes.Repeat([]byte("b"), 100)}
				seed, down := setupLoopbackFiles(t, files, []uint32{1}, ordered)
				completeTestDownload(t, down)
				if stale {
					if err := os.WriteFile(filepath.Join(down.Path, "a"), bytes.Repeat([]byte("x"), 100), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				uploaded := seed.GetUploadStats()
				if err := down.SetActiveFilesIDs([]uint32{0, 1}); err != nil {
					t.Fatal(err)
				}
				down.Wait()
				for name, want := range files {
					got, err := os.ReadFile(filepath.Join(down.Path, name))
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("expanded selected file %s was not restored", name)
					}
				}
				if seed.GetUploadStats() != uploaded {
					t.Fatal("cached boundary piece was downloaded again")
				}
				if _, err := down.getPieceInternal(0, true); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestDownloadSlotWaitCancellationAndRelease(t *testing.T) {
	s := &Server{}
	s.downloadMaxInflight.Store(1)
	if err := s.acquireDownloadSlot(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- s.acquireDownloadSlot(ctx) }()
	waitForTestCondition(t, time.Second, func() bool {
		s.downloadSlotsMu.Lock()
		defer s.downloadSlotsMu.Unlock()
		return s.downloadSlotsChanged != nil
	}, "blocked global slot")
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation: %v", err)
	}
	if s.downloadInflight.Load() != 1 {
		t.Fatal("cancellation changed existing reservations")
	}
	go func() { result <- s.acquireDownloadSlot(context.Background()) }()
	s.releaseDownloadSlot()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slot waiter was not awakened")
	}
	s.releaseDownloadSlot()
	if s.downloadInflight.Load() != 0 {
		t.Fatal("global slot leaked")
	}
}

func TestUnorderedPendingRetryAndRemoval(t *testing.T) {
	fetch := &PreFetcher{roundRobin: true, piecesList: []byte{0, 1, 0, 1}}
	peer := testPeerWithPieces(4, 1<<1|1<<3)
	fetch.initPendingPiecesLocked()
	fetch.reservePieceLocked(1)
	if len(fetch.pendingPieces) != 1 {
		t.Fatal("reserved piece remained pending")
	}
	fetch.markPieceForRetry(1)
	if len(fetch.pendingPieces) != 2 {
		t.Fatal("failed piece was not reinserted")
	}
	fetch.reservePieceLocked(3)
	piece, ok := fetch.nextPieceForPeer(peer, 1)
	if !ok || piece != 1 {
		t.Fatalf("pending retry not reachable: %d %v", piece, ok)
	}
}

type batchRecordingStore struct {
	recordingStorage
	batchCalls int
	batchErr   error
}

func (s *batchRecordingStore) SetPieces(_ []byte, pieces map[uint32]*PieceInfo) error {
	s.batchCalls++
	if s.syncFile != nil && !s.syncFile.synced {
		s.setPieceBeforeSync = true
	}
	return s.batchErr
}

func TestPieceCommitBatchPublishesOnlyAfterDurableSuccess(t *testing.T) {
	for _, fails := range []bool{false, true} {
		t.Run(fmt.Sprint(fails), func(t *testing.T) {
			file := &syncTrackingFile{}
			store := &batchRecordingStore{recordingStorage: recordingStorage{syncFile: file}}
			if fails {
				store.batchErr = errors.New("database write failed")
			}
			tor := &Torrent{db: store, pieceMask: make([]byte, 2), signalNewPieces: make(chan struct{}, 1)}
			committer := newPieceCommitter(tor)
			committer.MarkDirty(file)
			if err := committer.Add(2, 0, []byte{1}); err != nil {
				t.Fatal(err)
			}
			if err := committer.Add(9, 1, []byte{2}); err != nil {
				t.Fatal(err)
			}
			err := committer.Flush()
			if (err != nil) != fails {
				t.Fatalf("unexpected batch result: %v", err)
			}
			if store.batchCalls != 1 || store.setPieceCalls != 0 || store.setPieceBeforeSync {
				t.Fatalf("invalid batch durability/order: %+v", store)
			}
			if fails {
				if tor.DownloadedPiecesNum() != 0 || len(tor.newPieces) != 0 {
					t.Fatal("failed batch was published")
				}
				if len(committer.pending) != 2 {
					t.Fatal("failed batch was discarded")
				}
			} else if tor.DownloadedPiecesNum() != 2 || len(tor.newPieces) != 2 {
				t.Fatal("successful batch was not published")
			}
		})
	}
}

var benchmarkPendingPiece uint32
var benchmarkPendingOK bool

func BenchmarkUnorderedSparsePending(b *testing.B) {
	for _, count := range []int{10_000, 100_000, 1_000_000} {
		for _, available := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d_available_%v", count, available), func(b *testing.B) {
				fetch := &PreFetcher{roundRobin: true, piecesList: make([]byte, count)}
				fetch.piecesList[count-1] = 1
				fetch.initPendingPiecesLocked()
				peer := &storagePeer{}
				peer.resetPieceTrackingLocked(uint32(count))
				if available {
					bitsetSet(peer.hasPieces, uint32(count-1))
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchmarkPendingPiece, benchmarkPendingOK = fetch.nextPieceForPeer(peer, 1)
				}
			})
		}
	}
}

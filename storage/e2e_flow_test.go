package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
)

type e2eFileRef struct {
	path string
	name string
	size uint64
}

func (e e2eFileRef) GetName() string {
	return e.name
}

func (e e2eFileRef) GetSize() uint64 {
	return e.size
}

func (e e2eFileRef) CreateReader() (_ io.ReaderAt, _ func() error, err error) {
	f, err := os.Open(e.path)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

type e2eFSController struct{}

func (e *e2eFSController) AcquireRead(path string, p []byte, off int64) (n int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.ReadAt(p, off)
}

func (e *e2eFSController) RemoveFile(path string) error {
	return os.Remove(path)
}

type e2eFS struct {
	ctrl *e2eFSController
}

func (e *e2eFS) Open(name string, mode OpenMode) (FSFile, error) {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return nil, err
	}

	flags := os.O_RDONLY
	if mode == OpenModeWrite {
		flags = os.O_CREATE | os.O_RDWR
	}
	return os.OpenFile(name, flags, 0o644)
}

func (e *e2eFS) Delete(name string) error {
	return os.Remove(name)
}

func (e *e2eFS) Exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (e *e2eFS) GetController() FSController {
	return e.ctrl
}

type e2eStorage struct {
	mx             sync.RWMutex
	fs             *e2eFS
	torrents       map[string]*Torrent
	torrentsByOver map[string]*Torrent
	activeFiles    map[string][]uint32
	pieces         map[string]map[uint32]*PieceInfo
	forcePieceSize uint32
}

func newE2EStorage(forcePieceSize uint32) *e2eStorage {
	return &e2eStorage{
		fs:             &e2eFS{ctrl: &e2eFSController{}},
		torrents:       map[string]*Torrent{},
		torrentsByOver: map[string]*Torrent{},
		activeFiles:    map[string][]uint32{},
		pieces:         map[string]map[uint32]*PieceInfo{},
		forcePieceSize: forcePieceSize,
	}
}

func (e *e2eStorage) GetFS() FS {
	return e.fs
}

func (e *e2eStorage) GetAll() []*Torrent {
	return nil
}

func (e *e2eStorage) GetTorrentByOverlay(over []byte) *Torrent {
	e.mx.RLock()
	defer e.mx.RUnlock()

	return e.torrentsByOver[string(over)]
}

func (e *e2eStorage) SetTorrent(t *Torrent) error {
	overlayID, err := tl.Hash(keys.PublicKeyOverlay{Key: t.BagID})
	if err != nil {
		return err
	}

	e.mx.Lock()
	defer e.mx.Unlock()

	e.torrents[string(t.BagID)] = t
	e.torrentsByOver[string(overlayID)] = t
	e.torrentsByOver[string(t.BagID)] = t
	return nil
}

func (e *e2eStorage) SetActiveFiles(bagID []byte, ids []uint32) error {
	e.mx.Lock()
	defer e.mx.Unlock()

	cp := append([]uint32(nil), ids...)
	e.activeFiles[string(bagID)] = cp
	return nil
}

func (e *e2eStorage) GetActiveFiles(bagID []byte) ([]uint32, error) {
	e.mx.RLock()
	defer e.mx.RUnlock()

	return append([]uint32(nil), e.activeFiles[string(bagID)]...), nil
}

func (e *e2eStorage) GetPiece(bagID []byte, id uint32) (*PieceInfo, error) {
	e.mx.RLock()
	defer e.mx.RUnlock()

	bagPieces := e.pieces[string(bagID)]
	if bagPieces == nil {
		return nil, ErrFileNotExist
	}
	p := bagPieces[id]
	if p == nil {
		return nil, ErrFileNotExist
	}

	cp := *p
	cp.Proof = append([]byte(nil), p.Proof...)
	return &cp, nil
}

func (e *e2eStorage) RemovePiece(bagID []byte, id uint32) error {
	e.mx.Lock()
	defer e.mx.Unlock()

	if e.pieces[string(bagID)] != nil {
		delete(e.pieces[string(bagID)], id)
	}
	return nil
}

func (e *e2eStorage) SetPiece(bagID []byte, id uint32, p *PieceInfo) error {
	e.mx.Lock()
	defer e.mx.Unlock()

	if e.pieces[string(bagID)] == nil {
		e.pieces[string(bagID)] = map[uint32]*PieceInfo{}
	}
	cp := *p
	cp.Proof = append([]byte(nil), p.Proof...)
	e.pieces[string(bagID)][id] = &cp
	return nil
}

func (e *e2eStorage) PiecesMask(bagID []byte, num uint32) []byte {
	e.mx.RLock()
	defer e.mx.RUnlock()

	mask := make([]byte, piecesBitsetBytes(num))
	for id := range e.pieces[string(bagID)] {
		_ = bitsetSet(mask, id)
	}
	return mask
}

func (e *e2eStorage) UpdateUploadStats(_ []byte, _ uint64) error {
	return nil
}

func (e *e2eStorage) VerifyOnStartup() bool {
	return false
}

func (e *e2eStorage) GetForcedPieceSize() uint32 {
	return e.forcePieceSize
}

func activateTorrent(tor *Torrent, upload bool) {
	ctx, cancel := context.WithCancel(context.Background())
	tor.globalCtx = ctx
	tor.pause = cancel
	tor.activeUpload = upload
}

func freeUDPPort(t *testing.T) int {
	t.Helper()

	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve udp port: %v", err)
	}
	defer pc.Close()

	return pc.LocalAddr().(*net.UDPAddr).Port
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

type loopbackTorrentServer struct {
	id  []byte
	key ed25519.PrivateKey
}

func (l *loopbackTorrentServer) ConnectToNode(context.Context, *Torrent, *overlay.Node, *address.List) error {
	return nil
}

func (l *loopbackTorrentServer) GetADNLPrivateKey() ed25519.PrivateKey {
	return l.key
}

func (l *loopbackTorrentServer) GetID() []byte {
	return l.id
}

func (l *loopbackTorrentServer) Stop() {}

type loopbackADNL struct {
	remoteID  []byte
	remoteKey ed25519.PublicKey
	remote    *loopbackSide
	remoteAddr string
	ctx       context.Context
	cancel    context.CancelFunc

	customHandler     func(msg *adnl.MessageCustom) error
	queryHandler      func(msg *adnl.MessageQuery) error
	disconnectHandler func(addr string, key ed25519.PublicKey)
}

func newLoopbackADNL(remoteID []byte, remoteKey ed25519.PublicKey, remote *loopbackSide, remoteAddr string) *loopbackADNL {
	ctx, cancel := context.WithCancel(context.Background())
	return &loopbackADNL{
		remoteID:   remoteID,
		remoteKey:  remoteKey,
		remote:     remote,
		remoteAddr: remoteAddr,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (l *loopbackADNL) SetCustomMessageHandler(handler func(msg *adnl.MessageCustom) error) {
	l.customHandler = handler
}

func (l *loopbackADNL) SetQueryHandler(handler func(msg *adnl.MessageQuery) error) {
	l.queryHandler = handler
}

func (l *loopbackADNL) GetDisconnectHandler() func(addr string, key ed25519.PublicKey) {
	return l.disconnectHandler
}

func (l *loopbackADNL) SetDisconnectHandler(handler func(addr string, key ed25519.PublicKey)) {
	l.disconnectHandler = handler
}

func (l *loopbackADNL) SendCustomMessage(context.Context, tl.Serializable) error {
	return nil
}

func (l *loopbackADNL) Query(ctx context.Context, req, result tl.Serializable) error {
	inner, over := overlay.UnwrapQuery(req)
	switch q := inner.(type) {
	case Ping:
		remotePeer, sessionCtx := l.remote.torrent.prepareStoragePeer(over, nil, l.remote.conn, &q.SessionID)
		if sessionCtx != nil {
			go func() {
				_ = remotePeer.initializeSession(sessionCtx, atomic.LoadInt64(&remotePeer.sessionId), false)
			}()
		}
		remotePeer.touch()

		if res, ok := result.(*Pong); ok {
			*res = Pong{}
		}
		return nil
	case *Ping:
		remotePeer, sessionCtx := l.remote.torrent.prepareStoragePeer(over, nil, l.remote.conn, &q.SessionID)
		if sessionCtx != nil {
			go func() {
				_ = remotePeer.initializeSession(sessionCtx, atomic.LoadInt64(&remotePeer.sessionId), false)
			}()
		}
		remotePeer.touch()

		if res, ok := result.(*Pong); ok {
			*res = Pong{}
		}
		return nil
	case overlay.GetRandomPeers:
		if res, ok := result.(*overlay.NodesList); ok {
			*res = overlay.NodesList{}
		}
		return nil
	default:
		return fmt.Errorf("unsupported adnl query type %T", inner)
	}
}

func (l *loopbackADNL) Answer(context.Context, []byte, tl.Serializable) error {
	return nil
}

func (l *loopbackADNL) Ping(context.Context) (time.Duration, error) {
	return 0, nil
}

func (l *loopbackADNL) GetQueryHandler() func(msg *adnl.MessageQuery) error {
	return l.queryHandler
}

func (l *loopbackADNL) GetCloserCtx() context.Context {
	return l.ctx
}

func (l *loopbackADNL) SetAddresses(address.List) {}

func (l *loopbackADNL) RemoteAddr() string {
	return l.remoteAddr
}

func (l *loopbackADNL) GetID() []byte {
	return l.remoteID
}

func (l *loopbackADNL) GetPubKey() ed25519.PublicKey {
	return l.remoteKey
}

func (l *loopbackADNL) Reinit() {}

func (l *loopbackADNL) Close() {
	l.cancel()
}

type loopbackSide struct {
	torrent *Torrent
	conn    *PeerConnection
}

type loopbackRLDP struct {
	adnl   *loopbackADNL
	remote *loopbackSide
}

func (l *loopbackRLDP) GetADNL() rldp.ADNL {
	return l.adnl
}

func (l *loopbackRLDP) GetRateInfo() (left int64, total int64) {
	return 0, 0
}

func (l *loopbackRLDP) Close() {}

func (l *loopbackRLDP) DoQuery(ctx context.Context, _ uint64, query, result tl.Serializable) error {
	inner, over := overlay.UnwrapQuery(query)
	switch q := inner.(type) {
	case GetTorrentInfo:
		if l.remote.torrent.Info == nil {
			return fmt.Errorf("remote torrent info is nil")
		}
		c, err := tlb.ToCell(l.remote.torrent.Info)
		if err != nil {
			return err
		}
		if res, ok := result.(*TorrentInfoContainer); ok {
			*res = TorrentInfoContainer{Data: c.ToBOC()}
			return nil
		}
		return fmt.Errorf("unexpected torrent info result type %T", result)
	case *GetTorrentInfo:
		if l.remote.torrent.Info == nil {
			return fmt.Errorf("remote torrent info is nil")
		}
		c, err := tlb.ToCell(l.remote.torrent.Info)
		if err != nil {
			return err
		}
		if res, ok := result.(*TorrentInfoContainer); ok {
			*res = TorrentInfoContainer{Data: c.ToBOC()}
			return nil
		}
		return fmt.Errorf("unexpected torrent info result type %T", result)
	case AddUpdate:
		remotePeer, _ := l.remote.torrent.prepareStoragePeer(over, nil, l.remote.conn, &q.SessionID)
		remotePeer.touch()

		switch u := q.Update.(type) {
		case UpdateInit:
			piecesNum, err := remotePeer.waitForPiecesNum(ctx)
			if err != nil {
				return err
			}
			complete, err := remotePeer.applyInitChunk(piecesNum, uint32(u.HavePiecesOffset), u.HavePieces)
			if err != nil {
				return err
			}
			if complete {
				atomic.StoreInt32(&remotePeer.updateInitReceived, 1)
				l.remote.torrent.wake.fire()
			}
		case UpdateHavePieces:
			piecesNum, err := remotePeer.waitForPiecesNum(ctx)
			if err != nil {
				return err
			}
			remotePeer.piecesMx.Lock()
			remotePeer.ensurePieceTrackingLocked(piecesNum)
			for _, id := range u.PieceIDs {
				if id < 0 || uint32(id) >= piecesNum {
					remotePeer.piecesMx.Unlock()
					return fmt.Errorf("invalid piece id in update")
				}
				_ = bitsetSet(remotePeer.hasPieces, uint32(id))
			}
			remotePeer.piecesMx.Unlock()
			l.remote.torrent.wake.fire()
		case UpdateState:
		default:
			return fmt.Errorf("unsupported update type %T", q.Update)
		}

		if res, ok := result.(*Ok); ok {
			*res = Ok{}
			return nil
		}
		return fmt.Errorf("unexpected update result type %T", result)
	case *AddUpdate:
		remotePeer, _ := l.remote.torrent.prepareStoragePeer(over, nil, l.remote.conn, &q.SessionID)
		remotePeer.touch()

		switch u := q.Update.(type) {
		case UpdateInit:
			piecesNum, err := remotePeer.waitForPiecesNum(ctx)
			if err != nil {
				return err
			}
			complete, err := remotePeer.applyInitChunk(piecesNum, uint32(u.HavePiecesOffset), u.HavePieces)
			if err != nil {
				return err
			}
			if complete {
				atomic.StoreInt32(&remotePeer.updateInitReceived, 1)
				l.remote.torrent.wake.fire()
			}
		case UpdateHavePieces:
			piecesNum, err := remotePeer.waitForPiecesNum(ctx)
			if err != nil {
				return err
			}
			remotePeer.piecesMx.Lock()
			remotePeer.ensurePieceTrackingLocked(piecesNum)
			for _, id := range u.PieceIDs {
				if id < 0 || uint32(id) >= piecesNum {
					remotePeer.piecesMx.Unlock()
					return fmt.Errorf("invalid piece id in update")
				}
				_ = bitsetSet(remotePeer.hasPieces, uint32(id))
			}
			remotePeer.piecesMx.Unlock()
			l.remote.torrent.wake.fire()
		case UpdateState:
		default:
			return fmt.Errorf("unsupported update type %T", q.Update)
		}

		if res, ok := result.(*Ok); ok {
			*res = Ok{}
			return nil
		}
		return fmt.Errorf("unexpected update result type %T", result)
	case GetPiece:
		pc, err := l.remote.torrent.GetPiece(uint32(q.PieceID))
		if err != nil {
			return err
		}
		if remotePeer := l.remote.conn.GetFor(l.remote.torrent.BagID); remotePeer != nil {
			l.remote.torrent.UpdateUploadedPeer(remotePeer, uint64(len(pc.Data)))
		}
		if res, ok := result.(*Piece); ok {
			*res = *pc
			return nil
		}
		return fmt.Errorf("unexpected piece result type %T", result)
	case *GetPiece:
		pc, err := l.remote.torrent.GetPiece(uint32(q.PieceID))
		if err != nil {
			return err
		}
		if remotePeer := l.remote.conn.GetFor(l.remote.torrent.BagID); remotePeer != nil {
			l.remote.torrent.UpdateUploadedPeer(remotePeer, uint64(len(pc.Data)))
		}
		if res, ok := result.(*Piece); ok {
			*res = *pc
			return nil
		}
		return fmt.Errorf("unexpected piece result type %T", result)
	default:
		return fmt.Errorf("unsupported rldp query type %T", inner)
	}
}

func (l *loopbackRLDP) DoQueryAsync(ctx context.Context, maxAnswerSize uint64, id []byte, query tl.Serializable, result chan<- rldp.AsyncQueryResult) error {
	return fmt.Errorf("async loopback queries are not implemented")
}

func (l *loopbackRLDP) SetOnQuery(func(transferId []byte, query *rldp.Query) error) {}

func (l *loopbackRLDP) SetOnDisconnect(func()) {}

func (l *loopbackRLDP) SendAnswer(context.Context, uint64, uint32, []byte, []byte, tl.Serializable) error {
	return nil
}

func newLoopbackPeerConnection(id []byte, remoteID []byte, remoteKey ed25519.PublicKey, remote *loopbackSide, remoteAddr string) *PeerConnection {
	conn := &PeerConnection{
		usedByBags:    map[string]*storagePeer{},
		controlQueue:  make(chan struct{}, 4),
		dataQueue:     make(chan struct{}, 10),
		bagsInitQueue: make(chan struct{}, 8),
	}
	conn.MaxInflightPieces.Store(1)
	conn.srv = &Server{}
	conn.srv.downloadMaxInflight.Store(300)
	conn.adnl = newLoopbackADNL(remoteID, remoteKey, remote, remoteAddr)
	conn.rldp = &loopbackRLDP{
		adnl:   conn.adnl.(*loopbackADNL),
		remote: remote,
	}
	_ = id
	return conn
}

func TestE2E_TwoNodeDownloadFlow(t *testing.T) {
	prevLogger := Logger
	Logger = func(v ...any) {
		t.Log(v...)
	}
	defer func() {
		Logger = prevLogger
	}()

	seedDir := t.TempDir()
	downloadDir := t.TempDir()

	source := make([]byte, 64*1024+137)
	for i := range source {
		source[i] = byte((i*31 + 17) % 251)
	}

	sourceName := "payload.bin"
	sourcePath := filepath.Join(seedDir, sourceName)
	if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	_, seedKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate seeder key: %v", err)
	}
	seedGate := adnl.NewGateway(seedKey)
	seedPort := freeUDPPort(t)
	seedGate.SetAddressList([]*adnladdr.UDP{{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int32(seedPort),
	}})
	if err = seedGate.StartServer(fmt.Sprintf("127.0.0.1:%d", seedPort), 1); err != nil {
		t.Fatalf("failed to start seeder gateway: %v", err)
	}
	defer seedGate.Close()

	seedSrv := NewServer(nil, seedGate, seedKey, false, 1)
	defer seedSrv.Stop()

	seedStore := newE2EStorage(4096)
	seedConnector := NewConnector(seedSrv)
	seedSrv.SetStorage(seedStore)

	seedTorrent, err := CreateTorrentWithInitialHeader(
		context.Background(),
		seedDir,
		"e2e two-node download",
		&TorrentHeader{},
		seedStore,
		seedConnector,
		[]FileRef{e2eFileRef{
			path: sourcePath,
			name: sourceName,
			size: uint64(len(source)),
		}},
		nil,
		false,
	)
	if err != nil {
		t.Fatalf("failed to create seeder torrent: %v", err)
	}
	activateTorrent(seedTorrent, true)
	seedTorrent.downloadAll = true
	if err = seedStore.SetTorrent(seedTorrent); err != nil {
		t.Fatalf("failed to register seeder torrent: %v", err)
	}

	_, downKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate downloader key: %v", err)
	}
	downGate := adnl.NewGateway(downKey)
	if err = downGate.StartClient(1); err != nil {
		t.Fatalf("failed to start downloader gateway client: %v", err)
	}
	defer downGate.Close()

	downSrv := NewServer(nil, downGate, downKey, false, 1)
	defer downSrv.Stop()

	downStore := newE2EStorage(4096)
	downConnector := NewConnector(downSrv)
	downSrv.SetStorage(downStore)

	downTorrent := NewTorrent(downloadDir, downStore, downConnector)
	downTorrent.BagID = append([]byte(nil), seedTorrent.BagID...)
	downTorrent.downloadAll = true
	activateTorrent(downTorrent, false)
	if err = downStore.SetTorrent(downTorrent); err != nil {
		t.Fatalf("failed to register downloader torrent: %v", err)
	}

	doneCh := make(chan DownloadResult, 1)
	errCh := make(chan error, 1)
	if err = downTorrent.startDownload(func(event Event) {
		switch event.Name {
		case EventDone:
			select {
			case doneCh <- event.Value.(DownloadResult):
			default:
			}
		case EventErr:
			select {
			case errCh <- event.Value.(error):
			default:
			}
		}
	}); err != nil {
		t.Fatalf("failed to start downloader flow: %v", err)
	}

	seedNode, err := overlay.NewNode(seedTorrent.BagID, seedKey)
	if err != nil {
		t.Fatalf("failed to build overlay node: %v", err)
	}
	addrs := seedGate.GetAddressList()

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelConnect()
	if err = downSrv.ConnectToNode(connectCtx, downTorrent, seedNode, &addrs); err != nil {
		t.Fatalf("failed to connect downloader to seeder: %v", err)
	}

	waitUntil(t, 10*time.Second, func() bool {
		seedPeer := seedTorrent.GetPeer(downSrv.GetID())
		downPeer := downTorrent.GetPeer(seedSrv.GetID())
		return seedPeer != nil && seedPeer.peer != nil && seedPeer.peer.isSessionReady() &&
			downPeer != nil && downPeer.peer != nil && downPeer.peer.isSessionReady()
	}, "both nodes to finish session init")

	select {
	case err = <-errCh:
		t.Fatalf("download flow failed: %v", err)
	case <-doneCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for download completion")
	}

	downloadedPath := filepath.Join(downloadDir, sourceName)
	got, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if !bytes.Equal(got, source) {
		t.Fatal("downloaded file content mismatch")
	}
	if downTorrent.Header == nil || downTorrent.Info == nil {
		t.Fatal("expected downloader torrent metadata to be resolved")
	}
	if downTorrent.DownloadedPiecesNum() == 0 {
		t.Fatal("expected downloader to persist downloaded pieces")
	}
	if seedTorrent.GetUploadStats() == 0 {
		t.Fatal("expected seeder to report uploaded bytes")
	}

	downTorrent.Stop()
	seedTorrent.Stop()
	downTorrent.Wait()
	seedTorrent.Wait()
}

func TestLoopback_TwoNodeDownloadFlow(t *testing.T) {
	seedDir := t.TempDir()
	downloadDir := t.TempDir()

	source := make([]byte, 64*1024+137)
	for i := range source {
		source[i] = byte((i*31 + 17) % 251)
	}

	sourceName := "payload.bin"
	sourcePath := filepath.Join(seedDir, sourceName)
	if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	seedStore := newE2EStorage(4096)
	downStore := newE2EStorage(4096)

	_, seedKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate seeder key: %v", err)
	}
	_, downKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate downloader key: %v", err)
	}

	seedServer := &loopbackTorrentServer{
		id:  append([]byte(nil), seedKey.Public().(ed25519.PublicKey)...),
		key: seedKey,
	}
	downServer := &loopbackTorrentServer{
		id:  append([]byte(nil), downKey.Public().(ed25519.PublicKey)...),
		key: downKey,
	}

	seedConnector := NewConnector(seedServer)
	downConnector := NewConnector(downServer)

	seedTorrent, err := CreateTorrentWithInitialHeader(
		context.Background(),
		seedDir,
		"loopback two-node download",
		&TorrentHeader{},
		seedStore,
		seedConnector,
		[]FileRef{e2eFileRef{
			path: sourcePath,
			name: sourceName,
			size: uint64(len(source)),
		}},
		nil,
		false,
	)
	if err != nil {
		t.Fatalf("failed to create seeder torrent: %v", err)
	}
	activateTorrent(seedTorrent, true)
	seedTorrent.downloadAll = true
	if err = seedStore.SetTorrent(seedTorrent); err != nil {
		t.Fatalf("failed to register seeder torrent: %v", err)
	}

	downTorrent := NewTorrent(downloadDir, downStore, downConnector)
	downTorrent.BagID = append([]byte(nil), seedTorrent.BagID...)
	downTorrent.downloadAll = true
	activateTorrent(downTorrent, false)
	if err = downStore.SetTorrent(downTorrent); err != nil {
		t.Fatalf("failed to register downloader torrent: %v", err)
	}

	seedSide := &loopbackSide{torrent: seedTorrent}
	downSide := &loopbackSide{torrent: downTorrent}

	seedConn := newLoopbackPeerConnection(seedServer.id, downServer.id, downKey.Public().(ed25519.PublicKey), downSide, "loopback-downloader")
	downConn := newLoopbackPeerConnection(downServer.id, seedServer.id, seedKey.Public().(ed25519.PublicKey), seedSide, "loopback-seeder")
	seedSide.conn = seedConn
	downSide.conn = downConn

	downPeer, sessionCtx := downTorrent.prepareStoragePeer(seedTorrent.BagID, nil, downConn, nil)
	if sessionCtx == nil {
		t.Fatal("expected a fresh loopback session context")
	}
	if err = downPeer.initializeSession(sessionCtx, atomic.LoadInt64(&downPeer.sessionId), true); err != nil {
		t.Fatalf("failed to initialize loopback downloader session: %v", err)
	}
	downPeer.touch()

	waitUntil(t, 5*time.Second, func() bool {
		seedPeer := seedTorrent.GetPeer(downServer.id)
		downPeerInfo := downTorrent.GetPeer(seedServer.id)
		return seedPeer != nil && seedPeer.peer != nil && seedPeer.peer.isSessionReady() &&
			downPeerInfo != nil && downPeerInfo.peer != nil && downPeerInfo.peer.isSessionReady()
	}, "loopback session init on both sides")

	doneCh := make(chan DownloadResult, 1)
	errCh := make(chan error, 1)
	if err = downTorrent.startDownload(func(event Event) {
		switch event.Name {
		case EventDone:
			select {
			case doneCh <- event.Value.(DownloadResult):
			default:
			}
		case EventErr:
			select {
			case errCh <- event.Value.(error):
			default:
			}
		}
	}); err != nil {
		t.Fatalf("failed to start loopback downloader flow: %v", err)
	}

	select {
	case err = <-errCh:
		t.Fatalf("loopback download flow failed: %v", err)
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for loopback download completion")
	}

	downloadedPath := filepath.Join(downloadDir, sourceName)
	got, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if !bytes.Equal(got, source) {
		t.Fatal("downloaded file content mismatch")
	}
	if downTorrent.Header == nil || downTorrent.Info == nil {
		t.Fatal("expected downloader torrent metadata to be resolved")
	}
	if downTorrent.DownloadedPiecesNum() == 0 {
		t.Fatal("expected downloader to persist downloaded pieces")
	}
	if seedTorrent.GetUploadStats() == 0 {
		t.Fatal("expected seeder to report uploaded bytes")
	}

	downTorrent.Stop()
	seedTorrent.Stop()
	downTorrent.Wait()
	seedTorrent.Wait()
}

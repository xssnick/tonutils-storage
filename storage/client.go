package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/kevinms/leakybucket-go"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var Logger = func(...any) {}

const (
	sessionPingTimeout    = 7 * time.Second
	sessionPingAttempts   = 3
	sessionPingRetryDelay = 250 * time.Millisecond
)

type DHT interface {
	StoreAddress(ctx context.Context, addresses address.List, ttl time.Duration, ownerKey ed25519.PrivateKey) (int, []byte, error)
	StoreOverlayNodes(ctx context.Context, overlayKey []byte, nodes *overlay.NodesList, ttl time.Duration) (int, []byte, error)
	FindAddresses(ctx context.Context, key []byte) (*address.List, ed25519.PublicKey, error)
	FindOverlayNodes(ctx context.Context, overlayId []byte, continuation ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error)
	Close()
}

type FileInfo struct {
	Size            uint64
	FromPiece       uint32
	ToPiece         uint32
	FromPieceOffset uint32
	ToPieceOffset   uint32
	Index           uint32
	Name            string
}

type TorrentDownloader interface {
	Close()
	IsActive() bool
}

type torrentDownloader struct {
	globalCtx      context.Context
	downloadCancel func()
}

type peerSessionAttempt struct {
	ctx        context.Context
	id         int64
	generation uint64
}

type peerSessionState struct {
	sessionId       int64
	sessionSeqno    int64
	sessionGen      uint64
	localInitSent   int32
	sessionInitAt   int64
	lastInitChunkAt int64

	remoteInitComplete int32
	remoteStateKnown   int32
	remoteWillUpload   int32
	remoteWantDownload int32
	remoteSeqno        int64

	initScheduledGen uint64
}

type storagePeer struct {
	torrent        *Torrent
	nodeAddr       string
	overlay        []byte
	overlayNode    *overlay.Node
	nodeId         []byte
	conn           *PeerConnection
	lastActivityAt int64
	session        peerSessionState

	currentPing           int64
	lastPingAt            time.Time
	lastNeighboursAt      time.Time
	lastUpdatePiecesAt    int64
	lastUpdatePiecesSeqno int64
	lastSentNewPiecesPos  uint64

	lastSentPieces []byte
	hasPieces      []byte
	hasPiecesNum   uint32
	knownPieces    uint32

	initExpectedChunks uint32
	initReceivedChunks uint32
	initChunksMask     []byte
	pendingInitChunks  map[uint32][]byte
	pendingInitBytes   int
	pendingHavePieces  []int32

	piecesMx      sync.RWMutex
	prepareInfoMx sync.Mutex

	fails  int32
	failAt int64

	closeOnce sync.Once

	stopSession func()

	closerCtx context.Context
	stop      func()
}

const maxPendingInitBytes = 32 << 20
const maxPendingHavePieces = 1 << 20

var errStaleSessionAttempt = errors.New("stale session attempt")

func piecesBitsetBytes(bits uint32) int {
	if bits == 0 {
		return 0
	}
	return int((bits + 7) / 8)
}

func initChunksCount(piecesNum uint32) uint32 {
	maskBytes := piecesBitsetBytes(piecesNum)
	if maskBytes == 0 {
		return 0
	}

	chunks := maskBytes / maxPiecesBytesPerRequest
	if maskBytes%maxPiecesBytesPerRequest != 0 {
		chunks++
	}
	return uint32(chunks)
}

func bitsetHas(buf []byte, id uint32) bool {
	idx := id / 8
	if int(idx) >= len(buf) {
		return false
	}
	return buf[idx]&(1<<(id%8)) != 0
}

func bitsetSet(buf []byte, id uint32) bool {
	idx := id / 8
	if int(idx) >= len(buf) {
		return false
	}

	mask := byte(1 << (id % 8))
	if buf[idx]&mask != 0 {
		return false
	}
	buf[idx] |= mask
	return true
}

func countBitsetOnes(buf []byte) uint32 {
	var total uint32
	for _, b := range buf {
		total += uint32(bits.OnesCount8(b))
	}
	return total
}

func (p *storagePeer) configurePieceTrackingLocked(piecesNum uint32, clearPending bool) {
	p.hasPiecesNum = piecesNum
	p.hasPieces = make([]byte, piecesBitsetBytes(piecesNum))
	atomic.StoreUint32(&p.knownPieces, 0)
	p.initExpectedChunks = initChunksCount(piecesNum)
	p.initReceivedChunks = 0
	p.initChunksMask = make([]byte, piecesBitsetBytes(p.initExpectedChunks))
	if clearPending {
		p.pendingInitChunks = nil
		p.pendingInitBytes = 0
		p.pendingHavePieces = nil
	}
}

func (p *storagePeer) resetPieceTrackingLocked(piecesNum uint32) {
	p.configurePieceTrackingLocked(piecesNum, true)
}

func (p *storagePeer) ensurePieceTrackingLocked(piecesNum uint32) {
	if p.hasPiecesNum == piecesNum &&
		len(p.hasPieces) == piecesBitsetBytes(piecesNum) &&
		p.initExpectedChunks == initChunksCount(piecesNum) &&
		len(p.initChunksMask) == piecesBitsetBytes(p.initExpectedChunks) {
		return
	}
	p.configurePieceTrackingLocked(piecesNum, false)
}

func (p *storagePeer) waitForPiecesNum(ctx context.Context) (uint32, error) {
	for {
		p.torrent.mx.RLock()
		info := p.torrent.Info
		p.torrent.mx.RUnlock()
		if info != nil {
			return info.PiecesNum(), nil
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *peerSessionState) initProgressTimedOut(now time.Time, timeout time.Duration) bool {
	lastProgressAt := atomic.LoadInt64(&s.lastInitChunkAt)
	sessionInitAt := atomic.LoadInt64(&s.sessionInitAt)
	if lastProgressAt == 0 || lastProgressAt < sessionInitAt {
		lastProgressAt = sessionInitAt
	}
	if lastProgressAt == 0 {
		return true
	}
	return now.UnixMilli()-lastProgressAt > timeout.Milliseconds()
}

func (p *storagePeer) initProgressTimedOut(now time.Time, timeout time.Duration) bool {
	return p.session.initProgressTimedOut(now, timeout)
}

func newPeerSessionAttempt(ctx context.Context, id int64, generation uint64) *peerSessionAttempt {
	return &peerSessionAttempt{
		ctx:        ctx,
		id:         id,
		generation: generation,
	}
}

func (s *peerSessionState) currentAttempt(ctx context.Context) peerSessionAttempt {
	return peerSessionAttempt{
		ctx:        ctx,
		id:         atomic.LoadInt64(&s.sessionId),
		generation: atomic.LoadUint64(&s.sessionGen),
	}
}

func (p *storagePeer) currentSessionAttempt(ctx context.Context) peerSessionAttempt {
	return p.session.currentAttempt(ctx)
}

func (s *peerSessionState) isAttemptCurrent(attempt peerSessionAttempt) bool {
	return atomic.LoadInt64(&s.sessionId) == attempt.id &&
		atomic.LoadUint64(&s.sessionGen) == attempt.generation
}

func (p *storagePeer) isSessionAttemptCurrent(attempt peerSessionAttempt) bool {
	return p.session.isAttemptCurrent(attempt)
}

func boolToInt32(v bool) int32 {
	if v {
		return 1
	}
	return 0
}

func (s *peerSessionState) resetRemoteState() {
	atomic.StoreInt32(&s.remoteStateKnown, 0)
	atomic.StoreInt32(&s.remoteWillUpload, 0)
	atomic.StoreInt32(&s.remoteWantDownload, 0)
}

func (s *peerSessionState) setRemoteState(state State) {
	atomic.StoreInt32(&s.remoteWillUpload, boolToInt32(state.WillUpload))
	atomic.StoreInt32(&s.remoteWantDownload, boolToInt32(state.WantDownload))
	atomic.StoreInt32(&s.remoteStateKnown, 1)
}

func (p *storagePeer) setRemoteState(state State) {
	p.session.setRemoteState(state)
}

func (s *peerSessionState) remoteAllowsDownload() bool {
	return atomic.LoadInt32(&s.remoteStateKnown) == 0 ||
		atomic.LoadInt32(&s.remoteWillUpload) == 1
}

func (p *storagePeer) remoteAllowsDownload() bool {
	return p.session.remoteAllowsDownload()
}

func (s *peerSessionState) markLocalInitSent() {
	atomic.StoreInt32(&s.localInitSent, 1)
}

func (s *peerSessionState) markRemoteInitComplete() {
	atomic.StoreInt32(&s.remoteInitComplete, 1)
}

func (p *storagePeer) markRemoteInitComplete() {
	p.session.markRemoteInitComplete()
	p.torrent.wake.fire()
}

func (s *peerSessionState) resetForReinit(sessionID int64, now time.Time) uint64 {
	atomic.StoreInt64(&s.sessionInitAt, now.UnixMilli())
	atomic.StoreInt32(&s.localInitSent, 0)
	atomic.StoreInt32(&s.remoteInitComplete, 0)
	atomic.StoreInt64(&s.sessionId, sessionID)
	atomic.StoreInt64(&s.sessionSeqno, 0)
	atomic.StoreInt64(&s.remoteSeqno, 0)
	generation := atomic.AddUint64(&s.sessionGen, 1)
	atomic.StoreInt64(&s.lastInitChunkAt, 0)
	s.resetRemoteState()
	return generation
}

func (s *peerSessionState) acceptRemoteSeqno(seqno int64) bool {
	if seqno <= 0 {
		return false
	}
	for {
		current := atomic.LoadInt64(&s.remoteSeqno)
		if seqno <= current {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.remoteSeqno, current, seqno) {
			return true
		}
	}
}

func (s *peerSessionState) tryScheduleInit(attempt peerSessionAttempt) bool {
	if !s.isAttemptCurrent(attempt) {
		return false
	}

	for {
		scheduled := atomic.LoadUint64(&s.initScheduledGen)
		if scheduled >= attempt.generation {
			return false
		}
		if atomic.CompareAndSwapUint64(&s.initScheduledGen, scheduled, attempt.generation) {
			return true
		}
	}
}

func (p *storagePeer) torrentInfoSnapshot() *TorrentInfo {
	p.torrent.mx.RLock()
	defer p.torrent.mx.RUnlock()

	return p.torrent.Info
}

func (p *storagePeer) hasPiece(id uint32) bool {
	p.piecesMx.RLock()
	defer p.piecesMx.RUnlock()

	return p.hasPieceLocked(id)
}

// hasPieceLocked expects piecesMx to be held by the caller.
func (p *storagePeer) hasPieceLocked(id uint32) bool {
	if id >= p.hasPiecesNum {
		return false
	}
	return bitsetHas(p.hasPieces, id)
}

func (p *storagePeer) applyInitChunkLocked(piecesNum, off uint32, have []byte) (bool, error) {
	p.piecesMx.Lock()
	defer p.piecesMx.Unlock()
	return p.applyInitChunkUnsafe(piecesNum, off, have)
}

func (p *storagePeer) applyInitChunk(piecesNum, off uint32, have []byte) (bool, error) {
	p.piecesMx.Lock()
	defer p.piecesMx.Unlock()
	return p.applyInitChunkUnsafe(piecesNum, off, have)
}

func (p *storagePeer) applyInitChunkUnsafe(piecesNum, off uint32, have []byte) (bool, error) {
	if len(have) == 0 {
		return false, fmt.Errorf("empty init chunk")
	}

	p.ensurePieceTrackingLocked(piecesNum)

	if off >= piecesNum {
		return false, fmt.Errorf("invalid pieces offset")
	}

	chunkSpan := uint32(maxPiecesBytesPerRequest * 8)
	if off%chunkSpan != 0 {
		return false, fmt.Errorf("invalid init chunk offset")
	}

	chunkIdx := off / chunkSpan
	if chunkIdx >= p.initExpectedChunks {
		return false, fmt.Errorf("invalid init chunk index")
	}

	start := int(off / 8)
	expectedBytes := len(p.hasPieces) - start
	if expectedBytes > maxPiecesBytesPerRequest {
		expectedBytes = maxPiecesBytesPerRequest
	}
	if expectedBytes <= 0 || len(have) != expectedBytes {
		return false, fmt.Errorf("invalid init chunk size")
	}

	if rem := piecesNum % 8; rem != 0 && chunkIdx == p.initExpectedChunks-1 {
		validMask := byte((1 << rem) - 1)
		if have[len(have)-1]&^validMask != 0 {
			return false, fmt.Errorf("invalid trailing pieces bits")
		}
	}

	now := time.Now().UnixMilli()
	if !bitsetSet(p.initChunksMask, chunkIdx) {
		atomic.AddUint32(&p.knownPieces, mergeBitset(p.hasPieces[start:start+len(have)], have))
		atomic.StoreInt64(&p.session.lastInitChunkAt, now)
		return p.initExpectedChunks > 0 && p.initReceivedChunks == p.initExpectedChunks, nil
	}

	atomic.AddUint32(&p.knownPieces, mergeBitset(p.hasPieces[start:start+len(have)], have))
	p.initReceivedChunks++
	atomic.StoreInt64(&p.session.lastInitChunkAt, now)

	return p.initExpectedChunks > 0 && p.initReceivedChunks == p.initExpectedChunks, nil
}

func mergeBitset(dst, src []byte) uint32 {
	var added uint32
	for i, b := range src {
		old := dst[i]
		next := old | b
		added += uint32(bits.OnesCount8(next &^ old))
		dst[i] = next
	}
	return added
}

func (p *storagePeer) queuePendingInitChunk(off uint32, have []byte) error {
	p.piecesMx.Lock()
	defer p.piecesMx.Unlock()

	if len(have) == 0 {
		return fmt.Errorf("empty init chunk")
	}
	if len(have) > maxPiecesBytesPerRequest {
		return fmt.Errorf("invalid init chunk size")
	}

	chunkSpan := uint32(maxPiecesBytesPerRequest * 8)
	if off%chunkSpan != 0 {
		return fmt.Errorf("invalid init chunk offset")
	}

	if p.pendingInitChunks == nil {
		p.pendingInitChunks = map[uint32][]byte{}
	}

	if existing, ok := p.pendingInitChunks[off]; ok {
		if !bytes.Equal(existing, have) {
			return fmt.Errorf("conflicting init chunk")
		}
		atomic.StoreInt64(&p.session.lastInitChunkAt, time.Now().UnixMilli())
		return nil
	}

	if p.pendingInitBytes+len(have) > maxPendingInitBytes {
		return fmt.Errorf("too many pending init bytes")
	}

	copied := append([]byte(nil), have...)
	p.pendingInitChunks[off] = copied
	p.pendingInitBytes += len(copied)
	atomic.StoreInt64(&p.session.lastInitChunkAt, time.Now().UnixMilli())
	return nil
}

func (p *storagePeer) applyHavePiecesLocked(piecesNum uint32, pieceIDs []int32) error {
	p.ensurePieceTrackingLocked(piecesNum)
	for _, d := range pieceIDs {
		if d < 0 || uint32(d) >= piecesNum {
			return fmt.Errorf("invalid piece id in update")
		}

		if bitsetSet(p.hasPieces, uint32(d)) {
			atomic.AddUint32(&p.knownPieces, 1)
		}
	}
	return nil
}

func (p *storagePeer) queuePendingHavePieces(pieceIDs []int32) error {
	p.piecesMx.Lock()
	defer p.piecesMx.Unlock()

	for _, d := range pieceIDs {
		if d < 0 {
			return fmt.Errorf("invalid piece id in update")
		}
	}

	if len(p.pendingHavePieces)+len(pieceIDs) > maxPendingHavePieces {
		return fmt.Errorf("too many pending piece ids")
	}

	p.pendingHavePieces = append(p.pendingHavePieces, pieceIDs...)
	return nil
}

func (p *storagePeer) flushPendingPieceUpdates(piecesNum uint32) (bool, error) {
	p.piecesMx.Lock()
	defer p.piecesMx.Unlock()

	p.ensurePieceTrackingLocked(piecesNum)

	for off, have := range p.pendingInitChunks {
		if _, err := p.applyInitChunkUnsafe(piecesNum, off, have); err != nil {
			return false, err
		}
	}
	if len(p.pendingInitChunks) > 0 {
		p.pendingInitChunks = nil
		p.pendingInitBytes = 0
	}

	if len(p.pendingHavePieces) > 0 {
		if err := p.applyHavePiecesLocked(piecesNum, p.pendingHavePieces); err != nil {
			return false, err
		}
		p.pendingHavePieces = nil
	}

	return p.initExpectedChunks > 0 && p.initReceivedChunks == p.initExpectedChunks, nil
}

func (p *storagePeer) applySessionUpdate(update any) error {
	switch u := update.(type) {
	case UpdateInit:
		if u.HavePiecesOffset < 0 {
			return fmt.Errorf("invalid pieces offset")
		}
		off := uint32(u.HavePiecesOffset)
		p.setRemoteState(u.State)

		info := p.torrentInfoSnapshot()
		queued := false
		if info == nil {
			if err := p.queuePendingInitChunk(off, u.HavePieces); err != nil {
				return err
			}
			queued = true

			info = p.torrentInfoSnapshot()
			if info == nil {
				return nil
			}
		}

		complete, err := p.flushPendingPieceUpdates(info.PiecesNum())
		if err != nil {
			return err
		}
		if complete {
			p.markRemoteInitComplete()
		}
		if queued {
			return nil
		}

		complete, err = p.applyInitChunk(info.PiecesNum(), off, u.HavePieces)
		if err != nil {
			return err
		}
		if complete {
			p.markRemoteInitComplete()
		}
	case UpdateHavePieces:
		info := p.torrentInfoSnapshot()
		queued := false
		if info == nil {
			if err := p.queuePendingHavePieces(u.PieceIDs); err != nil {
				return err
			}
			queued = true

			info = p.torrentInfoSnapshot()
			if info == nil {
				return nil
			}
		}

		complete, err := p.flushPendingPieceUpdates(info.PiecesNum())
		if err != nil {
			return err
		}
		if complete {
			p.markRemoteInitComplete()
		}
		if queued {
			return nil
		}

		p.piecesMx.Lock()
		err = p.applyHavePiecesLocked(info.PiecesNum(), u.PieceIDs)
		p.piecesMx.Unlock()
		if err != nil {
			return err
		}
		p.torrent.wake.fire()
	case UpdateState:
		p.setRemoteState(u.State)
		p.torrent.wake.fire()
	default:
		return fmt.Errorf("unsupported update type %T", update)
	}

	return nil
}

type TorrentInfo struct {
	PieceSize   uint32   `tlb:"## 32"`
	FileSize    uint64   `tlb:"## 64"`
	RootHash    []byte   `tlb:"bits 256"`
	HeaderSize  uint64   `tlb:"## 64"`
	HeaderHash  []byte   `tlb:"bits 256"`
	Description tlb.Text `tlb:"."`
}

type SpeedLimit struct {
	bytesPerSec uint64
	bucket      unsafe.Pointer
}

type TorrentServer interface {
	ConnectToNode(ctx context.Context, t *Torrent, node *overlay.Node, addrs *address.List) error
	GetADNLPrivateKey() ed25519.PrivateKey
	GetID() []byte
	Stop()
}

type Connector struct {
	downloadLimit *SpeedLimit
	uploadLimit   *SpeedLimit
	TorrentServer
}

func NewConnector(srv TorrentServer) *Connector {
	return &Connector{
		TorrentServer: srv,
		downloadLimit: &SpeedLimit{},
		uploadLimit:   &SpeedLimit{},
	}
}

func (s *SpeedLimit) SetLimit(bytesPerSec uint64) {
	if bytesPerSec == 0 {
		atomic.StoreUint64(&s.bytesPerSec, 0)
		atomic.StorePointer(&s.bucket, unsafe.Pointer(nil))
		return
	}

	if bytesPerSec > math.MaxInt64/3 {
		bytesPerSec = math.MaxInt64 / 3
	}

	atomic.StoreUint64(&s.bytesPerSec, bytesPerSec)

	b := leakybucket.NewLeakyBucket(float64(bytesPerSec), int64(bytesPerSec*3))
	atomic.StorePointer(&s.bucket, unsafe.Pointer(b))
}

func (s *SpeedLimit) GetLimit() uint64 {
	return atomic.LoadUint64(&s.bytesPerSec)
}

func (s *SpeedLimit) Throttle(_ context.Context, sz uint64) error {
	b := (*leakybucket.LeakyBucket)(atomic.LoadPointer(&s.bucket))
	if b != nil {
		full := uint64(b.Capacity())
		if sz < full {
			full = sz
		}

		if b.Remaining() < int64(full) || b.Add(int64(sz)) == 0 {
			return fmt.Errorf("limited")
		}
	}
	return nil
}

func (c *Connector) GetUploadLimit() uint64 {
	return c.uploadLimit.GetLimit()
}

func (c *Connector) GetDownloadLimit() uint64 {
	return c.downloadLimit.GetLimit()
}

func (c *Connector) SetDownloadLimit(bytesPerSec uint64) {
	c.downloadLimit.SetLimit(bytesPerSec)
}

func (c *Connector) SetUploadLimit(bytesPerSec uint64) {
	c.uploadLimit.SetLimit(bytesPerSec)
}

func (c *Connector) ThrottleDownload(ctx context.Context, sz uint64) error {
	return c.downloadLimit.Throttle(ctx, sz)
}

func (c *Connector) ThrottleUpload(ctx context.Context, sz uint64) error {
	return c.uploadLimit.Throttle(ctx, sz)
}

func (c *Connector) GetADNLPrivateKey() ed25519.PrivateKey {
	return c.TorrentServer.GetADNLPrivateKey()
}

func (c *Connector) CreateDownloader(ctx context.Context, t *Torrent) (_ TorrentDownloader, err error) {
	if len(t.BagID) != 32 {
		return nil, fmt.Errorf("invalid torrent bag id")
	}

	globalCtx, downloadCancel := context.WithCancel(ctx)
	var dow = &torrentDownloader{
		globalCtx:      globalCtx,
		downloadCancel: downloadCancel,
	}
	defer func() {
		if err != nil {
			downloadCancel()
		}
	}()

	// connect to first node and resolve torrent info
	var info *TorrentInfo
	for info == nil {
		t.mx.RLock()
		info = t.Info
		t.mx.RUnlock()
		if info != nil {
			break
		}

		select {
		case <-ctx.Done():
			err = fmt.Errorf("failed to find storage nodes for this bag, err: %w", ctx.Err())
			return nil, err
		case <-time.After(10 * time.Millisecond):
		}
	}

	t.mx.RLock()
	headerLoaded := t.Header != nil
	t.mx.RUnlock()
	if !headerLoaded {
		hdrPieces := uint32(info.HeaderSize / uint64(info.PieceSize))
		if info.HeaderSize%uint64(info.PieceSize) > 0 {
			// add not full piece
			hdrPieces++
		}

		hdrMask := make([]byte, hdrPieces)
		for i := range hdrPieces {
			hdrMask[i] = 1
		}

		pf := NewPreFetcher(globalCtx, t, nil, hdrPieces, hdrMask)
		defer pf.Stop()

		data := make([]byte, 0, uint64(hdrPieces)*uint64(info.PieceSize))
		proofs := make([][]byte, 0, hdrPieces)
		for i := uint32(0); i < hdrPieces; i++ {
			piece, proof, pieceErr := pf.WaitGet(globalCtx, i)
			if pieceErr != nil {
				err = fmt.Errorf("failed to get header piece %d, err: %w", i, pieceErr)
				return nil, err
			}
			data = append(data, piece...)
			proofs = append(proofs, proof)
			pf.Free(i)
		}

		var header TorrentHeader
		data, err = tl.Parse(&header, data, true)
		if err != nil {
			err = fmt.Errorf("failed to load header from cell, err: %w", err)
			return nil, err
		}

		if header.FilesCount > maxTorrentHeaderFiles {
			return nil, fmt.Errorf("bag has > 1_000_000 files, looks dangerous")
		}
		if uint32(len(header.NameIndex)) != header.FilesCount ||
			uint32(len(header.DataIndex)) != header.FilesCount {
			err = fmt.Errorf("corrupted header, lack of files info")
			return nil, err
		}
		if err = header.validateStatic(); err != nil {
			err = fmt.Errorf("malicious or corrupted header: %w", err)
			return nil, err
		}

		t.mx.Lock()
		if t.Header == nil {
			t.Header = &header
		}
		t.mx.Unlock()

		for i, proof := range proofs {
			err = t.setPiece(uint32(i), &PieceInfo{
				StartFileIndex: 0,
				Proof:          proof,
			}, true)
			if err != nil {
				return nil, err
			}
		}
	}

	return dow, nil
}

func (p *storagePeer) Close() {
	p.closeOnce.Do(func() {
		Logger("[STORAGE] CLOSING CONNECTION OF", hex.EncodeToString(p.nodeId), p.nodeAddr, "BAG", hex.EncodeToString(p.torrent.BagID))
		p.stop()
		p.conn.CloseFor(p)
		p.torrent.RemovePeer(p.nodeId)
	})
}

func (p *storagePeer) isLocalInitSent() bool {
	return atomic.LoadInt32(&p.session.localInitSent) == 1
}

func (p *storagePeer) hasKnownPieces() bool {
	return atomic.LoadUint32(&p.knownPieces) > 0
}

func (p *storagePeer) isDownloadUsable() bool {
	return p.isLocalInitSent() && p.remoteAllowsDownload() &&
		(atomic.LoadInt32(&p.session.remoteInitComplete) == 1 || p.hasKnownPieces())
}

func (p *storagePeer) isSessionReady() bool {
	return p.isLocalInitSent() && atomic.LoadInt32(&p.session.remoteInitComplete) == 1
}

func (p *storagePeer) initializeSession(attempt *peerSessionAttempt, doPing bool) error {
	if attempt == nil {
		return fmt.Errorf("nil session attempt")
	}

	var err error
	defer func() {
		if err == nil {
			if !p.isSessionAttemptCurrent(*attempt) {
				return
			}
			p.session.markLocalInitSent()
			p.torrent.wake.fire()

			Logger("[STORAGE] SESSION INITIALIZED FOR", hex.EncodeToString(p.nodeId), "BAG", hex.EncodeToString(p.torrent.BagID), "SESSION", attempt.id, "GENERATION", attempt.generation)
			return
		}

		if !p.isSessionAttemptCurrent(*attempt) {
			return
		}

		Logger("[STORAGE] SESSION INITIALIZATION FAILED FOR", hex.EncodeToString(p.nodeId), "BAG", hex.EncodeToString(p.torrent.BagID), "SESSION", attempt.id, "GENERATION", attempt.generation, "ERR", err.Error())
		p.Close()
	}()

	if doPing {
		err = p.pingWithRetry(attempt.ctx)
		if err != nil {
			err = fmt.Errorf("failed to ping: %w", err)
			return err
		}
	}

	if err = p.prepareTorrentInfo(attempt.ctx); err != nil {
		err = fmt.Errorf("failed to prepare torrent info, err: %w", err)
		return err
	}

	if err = p.sendInitPieces(attempt.ctx, *attempt); err != nil {
		err = fmt.Errorf("failed to send init pieces, err: %w", err)
		return err
	}

	return nil
}

func (p *storagePeer) touch() {
	p.torrent.TouchPeer(p)
}

func (p *storagePeer) findNeighbours(ctx context.Context) (*overlay.NodesList, error) {
	var al overlay.NodesList
	err := p.conn.adnl.Query(ctx, overlay.WrapQuery(p.overlay, &overlay.GetRandomPeers{}), &al)
	if err != nil {
		return nil, err
	}
	return &al, nil
}

func (p *storagePeer) ping(ctx context.Context) error {
	ses := atomic.LoadInt64(&p.session.sessionId)
	if ses == 0 {
		return fmt.Errorf("no session id")
	}

	tm := time.Now()
	var pong Pong
	err := p.conn.adnl.Query(ctx, overlay.WrapQuery(p.overlay, &Ping{SessionID: ses}), &pong)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&p.currentPing, int64(time.Since(tm)/time.Millisecond))

	return nil
}

func (p *storagePeer) pingWithRetry(ctx context.Context) error {
	var lastErr error

	for attempt := 1; attempt <= sessionPingAttempts; attempt++ {
		qCtx, cancel := context.WithTimeout(ctx, sessionPingTimeout)
		err := p.ping(qCtx)
		cancel()
		if err == nil {
			return nil
		}

		lastErr = err
		if ctx.Err() != nil || attempt == sessionPingAttempts || err.Error() == "no session id" {
			break
		}

		Logger("[STORAGE] PING FAILED, RETRYING", attempt, "FOR", hex.EncodeToString(p.nodeId), p.nodeAddr, "BAG", hex.EncodeToString(p.torrent.BagID), "ERR", err.Error())

		retryDelay := sessionPingRetryDelay * time.Duration(attempt)
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return lastErr
}

func (p *storagePeer) downloadPiece(ctx context.Context, id uint32) (*Piece, int64, error) {
	tm := time.Now()

	var piece Piece
	err := func() error {
		err := p.conn.withDataQueueSlot(ctx, true, func() error {
			err := p.conn.rldp.DoQuery(ctx, 4096+uint64(p.torrent.Info.PieceSize)*2, overlay.WrapQuery(p.overlay, &GetPiece{int32(id)}), &piece)
			if err != nil {
				return fmt.Errorf("failed to query piece %d. err: %w", id, err)
			}

			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, ErrQueueIsBusy) {
				return fmt.Errorf("failed to acquire data queue slot: %w", err)
			}
			return err
		}

		proof, err := cell.FromBOC(piece.Proof)
		if err != nil {
			return fmt.Errorf("failed to parse BoC of piece %d, err: %w", id, err)
		}

		err = cell.CheckProof(proof, p.torrent.Info.RootHash)
		if err != nil {
			return fmt.Errorf("proof check of piece %d failed: %w", id, err)
		}

		err = p.torrent.checkProofBranch(proof, piece.Data, id)
		if err != nil {
			return fmt.Errorf("proof branch check of piece %d failed: %w", id, err)
		}

		p.torrent.UpdateDownloadedPeer(p, uint64(len(piece.Data)))

		return nil
	}()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, time.Since(tm).Milliseconds(), err
		}

		now := time.Now().Unix()
		if old := atomic.LoadInt64(&p.failAt); old < now-1 && atomic.CompareAndSwapInt64(&p.failAt, old, now) {
			// in case 3 fails with 2s delay in a row, disconnect
			if atomic.AddInt32(&p.fails, 1) >= 3 {
				Logger("[STORAGE] TOO MANY FAILS FROM", p.nodeAddr, "CLOSING CONNECTION, ERR:", err.Error())
				// something wrong, close connection, we should reconnect after it
				p.Close()
			}
		}
		return nil, time.Since(tm).Milliseconds(), err
	}
	atomic.StoreInt32(&p.fails, 0)
	atomic.StoreInt64(&p.failAt, 0)

	return &piece, time.Since(tm).Milliseconds(), nil
}

func (t *Torrent) checkProofBranch(proof *cell.Cell, data []byte, piece uint32) error {
	piecesNum := t.Info.PiecesNum()
	if piece >= piecesNum {
		return fmt.Errorf("piece is out of range %d/%d", piece, piecesNum)
	}

	tree, err := proof.PeekRef(0)
	if err != nil {
		return err
	}

	// calc tree depth
	depth := int(math.Log2(float64(piecesNum)))
	if piecesNum > uint32(math.Pow(2, float64(depth))) {
		// add 1 if pieces num is not exact log2
		depth++
	}

	// check bits from left to right and load branches
	for i := depth - 1; i >= 0; i-- {
		refId := 1
		if piece&(1<<i) == 0 {
			refId = 0
		}

		tree, err = tree.PeekRef(refId)
		if err != nil {
			return err
		}
	}

	leaf, err := tree.BeginParse()
	if err != nil {
		return err
	}
	bits, branchHash, err := leaf.RestBits()
	if err != nil {
		return err
	}
	if bits != merkleHashBits || len(branchHash) != merkleHashBits/8 {
		return fmt.Errorf("hash in not 32 bytes")
	}

	h := sha256.Sum256(data)
	if !bytes.Equal(branchHash, h[:]) {
		return fmt.Errorf("incorrect branch hash %s | %s", hex.EncodeToString(branchHash), hex.EncodeToString(h[:]))
	}
	return nil
}

func (t *torrentDownloader) Close() {
	t.downloadCancel()
}

func (t *torrentDownloader) IsActive() bool {
	select {
	case <-t.globalCtx.Done():
		return false
	default:
		return true
	}
}

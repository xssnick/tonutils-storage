package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/tl"
	"sort"
)

const maxTorrentHeaderFiles = 1_000_000

func init() {
	tl.Register(TorrentInfoContainer{}, "storage.torrentInfo data:bytes = storage.TorrentInfo")
	tl.Register(StorageError{}, "storage.error message:string = storage.Error")
	tl.Register(GetTorrentInfo{}, "storage.getTorrentInfo = storage.TorrentInfo")
	tl.Register(Piece{}, "storage.piece proof:bytes data:bytes = storage.Piece")
	tl.Register(GetPiece{}, "storage.getPiece piece_id:int = storage.Piece")
	tl.Register(Ping{}, "storage.ping session_id:long = storage.Pong")
	tl.Register(Pong{}, "storage.pong = storage.Pong")
	tl.Register(AddUpdate{}, "storage.addUpdate session_id:long seqno:int update:storage.Update = Ok")
	tl.Register(State{}, "storage.state will_upload:Bool want_download:Bool = storage.State")
	tl.Register(UpdateInit{}, "storage.updateInit have_pieces:bytes have_pieces_offset:int state:storage.State = storage.Update")
	tl.Register(UpdateHavePieces{}, "storage.updateHavePieces piece_id:(vector int) = storage.Update")
	tl.Register(UpdateState{}, "storage.updateState state:storage.State = storage.Update")
	tl.Register(Ok{}, "storage.ok = Ok")

	tl.Register(FECInfoNone{}, "fec_info_none#c82a1964 = FecInfo")
	tl.Register(TorrentHeader{}, "torrent_header#9128aab7 files_count:uint32 "+
		"tot_name_size:uint64 tot_data_size:uint64 fec:FecInfo "+
		"dir_name_size:uint32 dir_name:(dir_name_size * [uint8]) "+
		"name_index:(files_count * [uint64]) data_index:(files_count * [uint64]) "+
		"names:(file_names_size * [uint8]) data:(tot_data_size * [uint8]) "+
		"= TorrentHeader")
}

type AddUpdate struct {
	SessionID int64 `tl:"long"`
	Seqno     int64 `tl:"int"`
	Update    any   `tl:"struct boxed [storage.updateInit,storage.updateHavePieces,storage.updateState]"`
}

type TorrentInfoContainer struct {
	Data []byte `tl:"bytes"`
}

type GetTorrentInfo struct{}

type Piece struct {
	Proof []byte `tl:"bytes"`
	Data  []byte `tl:"bytes"`
}

type GetPiece struct {
	PieceID int32 `tl:"int"`
}

type Ping struct {
	SessionID int64 `tl:"long"`
}

type Pong struct{}

type State struct {
	WillUpload   bool `tl:"bool"`
	WantDownload bool `tl:"bool"`
}

type UpdateInit struct {
	HavePieces       []byte `tl:"bytes"`
	HavePiecesOffset int32  `tl:"int"`
	State            State  `tl:"struct boxed"`
}

type UpdateHavePieces struct {
	PieceIDs []int32 `tl:"vector int"`
}

type UpdateState struct {
	State State `tl:"struct boxed"`
}

type Ok struct{}

type StorageError struct {
	Message string `tl:"string"`
}

func (e StorageError) Error() string {
	return e.Message
}

type FECInfoNone struct{}

type TorrentHeader struct {
	FilesCount    uint32
	TotalNameSize uint64
	TotalDataSize uint64
	FEC           FECInfoNone
	DirNameSize   uint32
	DirName       []byte
	NameIndex     []uint64
	DataIndex     []uint64
	Names         []byte
	Data          []byte
}

func (t *TorrentHeader) Parse(data []byte) (_ []byte, err error) {
	*t = TorrentHeader{}

	// Manual parse because of not standard array definition
	if len(data) < 28 {
		return nil, fmt.Errorf("too short sizes data to parse")
	}
	t.FilesCount = binary.LittleEndian.Uint32(data)
	if t.FilesCount == 0 {
		return nil, fmt.Errorf("header has no files")
	}
	if t.FilesCount > maxTorrentHeaderFiles {
		return nil, fmt.Errorf("too many files in header")
	}
	data = data[4:]
	t.TotalNameSize = binary.LittleEndian.Uint64(data)
	data = data[8:]
	t.TotalDataSize = binary.LittleEndian.Uint64(data)
	data = data[8:]
	data, err = tl.Parse(&t.FEC, data, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse fec: %w", err)
	}
	t.DirNameSize = binary.LittleEndian.Uint32(data)
	data = data[4:]

	indexBytes := uint64(t.FilesCount) * 8
	arraysSize, ok := checkedAddUint64(uint64(t.DirNameSize), indexBytes, indexBytes, t.TotalNameSize, t.TotalDataSize)
	if !ok || uint64(len(data)) < arraysSize {
		return nil, fmt.Errorf("too short arrays data to parse")
	}

	t.DirName = data[:t.DirNameSize]
	data = data[t.DirNameSize:]

	t.NameIndex = make([]uint64, t.FilesCount)
	t.DataIndex = make([]uint64, t.FilesCount)
	for i := uint32(0); i < t.FilesCount; i++ {
		t.NameIndex[i] = binary.LittleEndian.Uint64(data[uint64(i)*8:])
		t.DataIndex[i] = binary.LittleEndian.Uint64(data[indexBytes+uint64(i)*8:])
	}
	data = data[indexBytes*2:]

	t.Names = data[:t.TotalNameSize]
	data = data[t.TotalNameSize:]
	t.Data = data[:t.TotalDataSize]
	data = data[t.TotalDataSize:]

	if err = t.validateStatic(); err != nil {
		return nil, err
	}
	return data, nil
}

func checkedAddUint64(vals ...uint64) (uint64, bool) {
	var total uint64
	for _, v := range vals {
		if total+v < total {
			return 0, false
		}
		total += v
	}
	return total, true
}

func (t *TorrentHeader) validateStatic() error {
	if err := t.validateShape(); err != nil {
		return err
	}

	var prevName uint64
	for i, end := range t.NameIndex {
		if end < prevName {
			return fmt.Errorf("corrupted header, non-monotonic name index %d", i)
		}
		if end > uint64(len(t.Names)) {
			return fmt.Errorf("corrupted header, too short names data")
		}
		name := string(t.Names[prevName:end])
		if err := validateFileName(name, true); err != nil {
			return fmt.Errorf("malicious file name %q: %w", name, err)
		}
		prevName = end
	}
	if prevName != uint64(len(t.Names)) {
		return fmt.Errorf("corrupted header, unindexed names data")
	}

	var prevData uint64
	for i, end := range t.DataIndex {
		if end < prevData {
			return fmt.Errorf("corrupted header, non-monotonic data index %d", i)
		}
		prevData = end
	}
	return nil
}

func (t *TorrentHeader) validateShape() error {
	if t == nil {
		return fmt.Errorf("header is nil")
	}
	if t.FilesCount == 0 {
		return fmt.Errorf("header has no files")
	}
	if t.FilesCount > maxTorrentHeaderFiles {
		return fmt.Errorf("too many files in header")
	}
	if uint32(len(t.NameIndex)) != t.FilesCount || uint32(len(t.DataIndex)) != t.FilesCount {
		return fmt.Errorf("corrupted header, lack of files info")
	}
	if uint64(len(t.Names)) != t.TotalNameSize {
		return fmt.Errorf("corrupted header, incorrect names size")
	}
	if uint64(len(t.Data)) != t.TotalDataSize {
		return fmt.Errorf("corrupted header, incorrect data size")
	}
	if t.DirNameSize != uint32(len(t.DirName)) {
		return fmt.Errorf("corrupted header, incorrect dir name size")
	}
	if len(t.DirName) > 256 {
		return fmt.Errorf("too big dir name > 256")
	}
	if err := validateFileName(string(t.DirName), false); err != nil {
		return fmt.Errorf("malicious dir name: %w", err)
	}
	return nil
}

func (t *TorrentHeader) Serialize(buffer *bytes.Buffer) error {
	tmp := make([]byte, 20)
	binary.LittleEndian.PutUint32(tmp[0:], t.FilesCount)
	binary.LittleEndian.PutUint64(tmp[4:], t.TotalNameSize)
	binary.LittleEndian.PutUint64(tmp[12:], t.TotalDataSize)
	buffer.Write(tmp)

	fecData, err := tl.Serialize(t.FEC, true)
	if err != nil {
		return err
	}
	buffer.Write(fecData)

	if t.DirNameSize != uint32(len(t.DirName)) {
		return fmt.Errorf("incorrect dir name size")
	}

	dataDirNameSz := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataDirNameSz, t.DirNameSize)
	buffer.Write(dataDirNameSz)
	buffer.Write(t.DirName)

	for _, ni := range t.NameIndex {
		iData := make([]byte, 8)
		binary.LittleEndian.PutUint64(iData, ni)
		buffer.Write(iData)
	}

	for _, ni := range t.DataIndex {
		iData := make([]byte, 8)
		binary.LittleEndian.PutUint64(iData, ni)
		buffer.Write(iData)
	}
	buffer.Write(t.Names)
	buffer.Write(t.Data)

	return nil
}

func (t *Torrent) calcFileIndexes() error {
	t.mx.Lock()
	defer t.mx.Unlock()

	// already calculated
	if t.filesIndex != nil {
		return nil
	}

	filesIndex := map[string]uint32{}
	for i := uint32(0); i < t.Header.FilesCount; i++ {
		name, _, _, err := t.fileBoundsByID(i)
		if err != nil {
			return err
		}
		filesIndex[name] = i
	}
	t.filesIndex = filesIndex
	return nil
}

var ErrFileNotExist = errors.New("file is not exists in torrent")

func (t *Torrent) GetFileOffsets(name string) (*FileInfo, error) {
	if err := t.calcFileIndexes(); err != nil {
		return nil, err
	}

	i, ok := t.filesIndex[name]
	if !ok {
		return nil, ErrFileNotExist
	}
	return t.GetFileOffsetsByID(i)
}

func (t *Torrent) GetFilesInPiece(piece uint32) ([]*FileInfo, error) {
	start := uint64(piece) * uint64(t.Info.PieceSize)
	end := uint64(piece+1) * uint64(t.Info.PieceSize)
	if end <= t.Info.HeaderSize {
		return nil, nil
	}

	dataStart := uint64(0)
	if start > t.Info.HeaderSize {
		dataStart = start - t.Info.HeaderSize
	}
	dataEnd := end - t.Info.HeaderSize

	var files []*FileInfo
	first := sort.Search(len(t.Header.DataIndex), func(i int) bool {
		return t.Header.DataIndex[i] > dataStart
	})
	for i := first; i < len(t.Header.DataIndex); i++ {
		fileStart, fileEnd := uint64(0), t.Header.DataIndex[i]
		if i > 0 {
			fileStart = t.Header.DataIndex[i-1]
		}

		if fileStart >= dataEnd {
			break
		}
		if fileEnd <= dataStart {
			continue
		}

		file, err := t.GetFileOffsetsByID(uint32(i))
		if err != nil {
			return nil, fmt.Errorf("failed to get offsets for %d: %w", i, err)
		}
		files = append(files, file)
	}

	return files, nil
}

func (t *Torrent) GetFileOffsetsByID(i uint32) (*FileInfo, error) {
	name, start, end, err := t.fileBoundsByID(i)
	if err != nil {
		return nil, err
	}
	info := &FileInfo{
		Index: i,
	}

	absStart := t.Info.HeaderSize + start
	absEnd := t.Info.HeaderSize + end
	pieceSize := uint64(t.Info.PieceSize)

	info.FromPiece = uint32(absStart / pieceSize)
	info.FromPieceOffset = uint32(absStart % pieceSize)
	if absEnd == absStart {
		info.ToPiece = info.FromPiece
		info.ToPieceOffset = info.FromPieceOffset
	} else {
		lastByte := absEnd - 1
		info.ToPiece = uint32(lastByte / pieceSize)
		info.ToPieceOffset = uint32(absEnd - uint64(info.ToPiece)*pieceSize)
	}
	info.Size = end - start
	info.Name = name

	return info, nil
}

func (t *Torrent) fileBoundsByID(i uint32) (name string, start uint64, end uint64, err error) {
	if t.Header == nil || t.Info == nil {
		return "", 0, 0, fmt.Errorf("torrent metadata is not loaded")
	}
	if t.Info.PieceSize == 0 || t.Info.FileSize < t.Info.HeaderSize {
		return "", 0, 0, fmt.Errorf("corrupted torrent info sizes")
	}
	if err = t.Header.validateShape(); err != nil {
		return "", 0, 0, err
	}
	if i >= t.Header.FilesCount || int(i) >= len(t.Header.DataIndex) || int(i) >= len(t.Header.NameIndex) {
		return "", 0, 0, ErrFileNotExist
	}

	end = t.Header.DataIndex[i]
	if i > 0 {
		start = t.Header.DataIndex[i-1]
	}
	if end < start {
		return "", 0, 0, fmt.Errorf("corrupted header, non-monotonic data index")
	}
	if end > t.Info.FileSize-t.Info.HeaderSize {
		return "", 0, 0, fmt.Errorf("corrupted header, data out of range")
	}

	nameFrom := uint64(0)
	if i > 0 {
		nameFrom = t.Header.NameIndex[i-1]
	}
	nameTo := t.Header.NameIndex[i]
	if nameTo < nameFrom || nameTo > uint64(len(t.Header.Names)) {
		return "", 0, 0, fmt.Errorf("corrupted header, names data out of range")
	}
	name = string(t.Header.Names[nameFrom:nameTo])
	if err = validateFileName(name, true); err != nil {
		return "", 0, 0, fmt.Errorf("malicious file name %q: %w", name, err)
	}

	return name, start, end, nil
}

func (t *Torrent) ListFiles() ([]string, error) {
	if err := t.calcFileIndexes(); err != nil {
		return nil, err
	}

	files := make([]string, len(t.filesIndex), len(t.filesIndex))
	for s, idx := range t.filesIndex {
		files[idx] = s
	}
	return files, nil
}

package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/tl"
)

func init() {
	tl.Register(TorrentInfoContainer{}, "storage.torrentInfo data:bytes = storage.TorrentInfo")
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

	tl.Register(ADNLProofScheme{}, "storage.tonutils.adnlProviderProof provider_key:int256 = storage.tonutils.AdnlProviderProof")
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

type FECInfoNone struct{}

type ADNLProofScheme struct {
	Key []byte `tl:"int256"`
}

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
	// Manual parse because of not standard array definition
	if len(data) < 28 {
		return nil, fmt.Errorf("too short sizes data to parse")
	}
	t.FilesCount = binary.LittleEndian.Uint32(data)
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

	if uint64(len(data)) < uint64(t.DirNameSize)+uint64(t.FilesCount*8*2)+t.TotalNameSize+t.TotalDataSize {
		return nil, fmt.Errorf("too short arrays data to parse")
	}

	t.DirName = data[:t.DirNameSize]
	data = data[t.DirNameSize:]

	for i := uint32(0); i < t.FilesCount; i++ {
		t.NameIndex = append(t.NameIndex, binary.LittleEndian.Uint64(data[i*8:]))
		t.DataIndex = append(t.DataIndex, binary.LittleEndian.Uint64(data[t.FilesCount*8+i*8:]))
	}
	data = data[t.FilesCount*8*2:]

	t.Names = data[:t.TotalNameSize]
	data = data[t.TotalNameSize:]
	t.Data = data[:t.TotalDataSize]
	data = data[t.TotalDataSize:]
	return data, nil
}

func (t *TorrentHeader) Serialize() ([]byte, error) {
	data := make([]byte, 20)
	binary.LittleEndian.PutUint32(data[0:], t.FilesCount)
	binary.LittleEndian.PutUint64(data[4:], t.TotalNameSize)
	binary.LittleEndian.PutUint64(data[12:], t.TotalDataSize)

	fecData, err := tl.Serialize(t.FEC, true)
	if err != nil {
		return nil, err
	}
	data = append(data, fecData...)

	if t.DirNameSize != uint32(len(t.DirName)) {
		return nil, fmt.Errorf("incorrect dir name size")
	}

	dataDirNameSz := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataDirNameSz, t.DirNameSize)
	data = append(data, dataDirNameSz...)
	data = append(data, t.DirName...)

	for _, ni := range t.NameIndex {
		iData := make([]byte, 8)
		binary.LittleEndian.PutUint64(iData, ni)
		data = append(data, iData...)
	}

	for _, ni := range t.DataIndex {
		iData := make([]byte, 8)
		binary.LittleEndian.PutUint64(iData, ni)
		data = append(data, iData...)
	}
	data = append(data, t.Names...)
	data = append(data, t.Data...)

	return data, nil
}

func (t *Torrent) calcFileIndexes() error {
	t.mx.Lock()
	defer t.mx.Unlock()

	// already calculated
	if t.filesIndex != nil {
		return nil
	}

	t.filesIndex = map[string]uint32{}
	for i := uint32(0); i < t.Header.FilesCount; i++ {
		if uint64(len(t.Header.Names)) < t.Header.NameIndex[i] {
			return fmt.Errorf("corrupted header, too short names data")
		}
		if t.Info.FileSize < t.Header.DataIndex[i]+t.Info.HeaderSize {
			return fmt.Errorf("corrupted header, data out of range")
		}

		nameFrom := uint64(0)
		if i > 0 {
			nameFrom = t.Header.NameIndex[i-1]
		}
		name := t.Header.Names[nameFrom:t.Header.NameIndex[i]]
		t.filesIndex[string(name)] = i
	}
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

	var files []*FileInfo
	for i := range t.Header.DataIndex {
		fileStart, fileEnd := uint64(0), t.Header.DataIndex[i]
		if i > 0 {
			fileStart = t.Header.DataIndex[i-1]
		}
		fileStart += t.Info.HeaderSize
		fileEnd += t.Info.HeaderSize

		if fileStart >= end {
			break
		}
		if fileEnd < start {
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
	if int(i) >= len(t.Header.DataIndex) {
		return nil, ErrFileNotExist
	}
	info := &FileInfo{
		Index: i,
	}

	var end = t.Header.DataIndex[i]
	var start uint64 = 0
	if i > 0 {
		start = t.Header.DataIndex[i-1]
	}
	info.FromPiece = uint32((t.Info.HeaderSize + start) / uint64(t.Info.PieceSize))
	info.ToPiece = uint32((t.Info.HeaderSize + end) / uint64(t.Info.PieceSize))
	info.FromPieceOffset = uint32((t.Info.HeaderSize + start) - uint64(info.FromPiece)*uint64(t.Info.PieceSize))
	info.ToPieceOffset = uint32((t.Info.HeaderSize + end) - uint64(info.ToPiece)*uint64(t.Info.PieceSize))
	info.Size = (uint64(info.ToPiece-info.FromPiece)*uint64(t.Info.PieceSize) + uint64(info.ToPieceOffset)) - uint64(info.FromPieceOffset)

	var nameFrom uint64 = 0
	if i > 0 {
		nameFrom = t.Header.NameIndex[i-1]
	}
	info.Name = string(t.Header.Names[nameFrom:t.Header.NameIndex[i]])

	return info, nil
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

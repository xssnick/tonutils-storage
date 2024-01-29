package db

import (
	"bytes"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xssnick/tonutils-storage/storage"
)

func (s *Storage) SetActiveFiles(bagId []byte, ids []uint32) error {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32)
	copy(k, "ai:")
	copy(k[3:3+32], bagId)

	v := make([]byte, len(ids)*4)
	for i := 0; i < len(ids); i++ {
		binary.LittleEndian.PutUint32(v[i*4:], ids[i])
	}
	defer s.notify(EventTorrentUpdated)

	return s.db.Put(k, v, nil)
}

func (s *Storage) GetActiveFiles(bagId []byte) ([]uint32, error) {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32)
	copy(k, "ai:")
	copy(k[3:3+32], bagId)

	res, err := s.db.Get(k, nil)
	if err != nil {
		return nil, err
	}

	var files = make([]uint32, len(res)/4)
	for i := 0; i < len(res)/4; i++ {
		files[i] = binary.LittleEndian.Uint32(res[i*4:])
	}
	return files, nil
}

func (s *Storage) GetPiece(bagId []byte, id uint32) (*storage.PieceInfo, error) {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32+4)
	copy(k, "pc:")
	copy(k[3:3+32], bagId)
	binary.LittleEndian.PutUint32(k[3+32:], id)

	res, err := s.db.Get(k, nil)
	if err != nil {
		return nil, err
	}

	return &storage.PieceInfo{
		StartFileIndex: binary.LittleEndian.Uint32(res),
		Proof:          res[4:],
	}, nil
}

func (s *Storage) RemovePiece(bagId []byte, id uint32) error {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32+4)
	copy(k, "pc:")
	copy(k[3:3+32], bagId)
	binary.LittleEndian.PutUint32(k[3+32:], id)

	defer s.notify(EventTorrentUpdated)
	return s.db.Delete(k, nil)
}

func (s *Storage) SetPiece(bagId []byte, id uint32, p *storage.PieceInfo) error {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32+4)
	copy(k, "pc:")
	copy(k[3:3+32], bagId)
	binary.LittleEndian.PutUint32(k[3+32:], id)

	v := make([]byte, 4+len(p.Proof))
	binary.LittleEndian.PutUint32(v, p.StartFileIndex)
	copy(v[4:], p.Proof)

	defer s.notify(EventTorrentUpdated)
	return s.db.Put(k, v, nil)
}

func (s *Storage) PiecesMask(bagId []byte, num uint32) []byte {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32)
	copy(k, "pc:")
	copy(k[3:3+32], bagId)

	p := num / 8
	if num%8 != 0 {
		p++
	}

	mask := make([]byte, p)
	iter := s.db.NewIterator(&util.Range{Start: k}, nil)
	for iter.Next() {
		if !bytes.HasPrefix(iter.Key(), k) {
			break
		}
		id := binary.LittleEndian.Uint32(iter.Key()[len(k):])
		mask[id/8] |= 1 << (id % 8)
	}
	return mask
}

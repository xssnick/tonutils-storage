package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xssnick/tonutils-storage/storage"
	"math/big"
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

func (s *Storage) UpdateStats(bagId []byte, stats *storage.TorrentStats) error {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32)
	copy(k, "st:")
	copy(k[3:3+32], bagId)

	e := stats.Earned.Bytes()
	p := stats.Paid.Bytes()

	v := make([]byte, 2+len(e)+len(p))
	v[0] = byte(len(e))
	copy(v[1:], e)
	v[1+len(e)] = byte(len(p))
	copy(v[2+len(e):], p)

	return s.db.Put(k, v, nil)
}

func (s *Storage) LoadStats(bagId []byte) (*storage.TorrentStats, error) {
	if len(bagId) != 32 {
		panic("invalid bag id len, should be 32")
	}

	k := make([]byte, 3+32)
	copy(k, "st:")
	copy(k[3:3+32], bagId)

	v, err := s.db.Get(k, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return &storage.TorrentStats{
				Paid:   big.NewInt(0),
				Earned: big.NewInt(0),
			}, nil
		}
		return nil, err
	}

	return &storage.TorrentStats{
		Earned: new(big.Int).SetBytes(v[1 : 1+v[0]]),
		Paid:   new(big.Int).SetBytes(v[1+v[0] : 2+v[0]+v[1+v[0]]]),
	}, nil
}

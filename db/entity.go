package db

import (
	"encoding/binary"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
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

	if len(res) < 4 {
		return nil, fmt.Errorf("corrupted piece %d metadata", id)
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

// SetPieces stores one commit group's metadata in a single database write.
func (s *Storage) SetPieces(bagID []byte, pieces map[uint32]*storage.PieceInfo) error {
	if len(bagID) != 32 {
		return fmt.Errorf("invalid bag id len, should be 32")
	}
	if len(pieces) == 0 {
		return nil
	}
	batch := new(leveldb.Batch)
	for id, piece := range pieces {
		if piece == nil {
			return fmt.Errorf("piece %d metadata is nil", id)
		}
		key := make([]byte, 3+32+4)
		copy(key, "pc:")
		copy(key[3:], bagID)
		binary.LittleEndian.PutUint32(key[3+32:], id)
		value := make([]byte, 4+len(piece.Proof))
		binary.LittleEndian.PutUint32(value, piece.StartFileIndex)
		copy(value[4:], piece.Proof)
		batch.Put(key, value)
	}
	if err := s.db.Write(batch, nil); err != nil {
		return err
	}
	s.notify(EventTorrentUpdated)
	return nil
}

func (s *Storage) PiecesMask(bagId []byte, num uint32) []byte {
	mask, err := s.PiecesMaskWithError(bagId, num)
	if err != nil {
		log.Error().Err(err).Msg("failed to load pieces mask")
	}
	return mask
}

func (s *Storage) PiecesMaskWithError(bagId []byte, num uint32) ([]byte, error) {
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
	iter := s.db.NewIterator(util.BytesPrefix(k), nil)
	defer iter.Release()
	for iter.Next() {
		key := iter.Key()
		if len(key) != len(k)+4 {
			return mask, fmt.Errorf("corrupted piece key")
		}
		id := binary.LittleEndian.Uint32(key[len(k):])
		if id >= num {
			return mask, fmt.Errorf("piece %d is outside torrent's %d pieces", id, num)
		}
		mask[id/8] |= 1 << (id % 8)
	}
	return mask, iter.Error()
}

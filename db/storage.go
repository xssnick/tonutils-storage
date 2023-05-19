package db

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-storage/storage"
	"sync"
)

type Config struct {
	Key           ed25519.PrivateKey
	ListenAddr    string
	ExternalIP    string
	DownloadsPath string
}

type Storage struct {
	torrents        map[string]*storage.Torrent
	torrentsOverlay map[string]*storage.Torrent

	db *leveldb.DB
	mx sync.RWMutex
}

func NewStorage(db *leveldb.DB) (*Storage, error) {
	s := &Storage{
		torrents:        map[string]*storage.Torrent{},
		torrentsOverlay: map[string]*storage.Torrent{},
		db:              db,
	}

	err := s.loadTorrents()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) GetTorrent(hash []byte) *storage.Torrent {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.torrents[string(hash)]
}

func (s *Storage) GetTorrentByOverlay(overlay []byte) *storage.Torrent {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.torrentsOverlay[string(overlay)]
}

func (s *Storage) GetAll() []*storage.Torrent {
	s.mx.RLock()
	defer s.mx.RUnlock()

	res := make([]*storage.Torrent, 0, len(s.torrents))
	for _, t := range s.torrents {
		res = append(res, t)
	}
	return res
}

func (s *Storage) SetTorrent(t *storage.Torrent) error {
	data, err := json.Marshal(&TorrentStored{
		BagID:  t.BagID,
		Path:   t.Path,
		Info:   t.Info,
		Header: t.Header,
		Active: t.IsActive(),
	})
	if err != nil {
		return err
	}

	k := make([]byte, 5+32)
	copy(k, "bags:")
	copy(k[5:], t.BagID)

	err = s.db.Put(k, data, nil)
	if err != nil {
		return err
	}

	return s.addTorrent(t)
}

func (s *Storage) addTorrent(t *storage.Torrent) error {
	id, err := adnl.ToKeyID(adnl.PublicKeyOverlay{Key: t.BagID})
	if err != nil {
		return err
	}
	s.mx.Lock()
	s.torrents[string(t.BagID)] = t
	s.torrentsOverlay[string(id)] = t
	s.mx.Unlock()
	return nil
}

type TorrentStored struct {
	BagID  []byte
	Path   string
	Info   *storage.TorrentInfo
	Header *storage.TorrentHeader
	Active bool
}

func (s *Storage) loadTorrents() error {
	iter := s.db.NewIterator(&util.Range{Start: []byte("bags:")}, nil)
	for iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("bags:")) {
			break
		}

		var tr TorrentStored
		err := json.Unmarshal(iter.Value(), &tr)
		if err != nil {
			return fmt.Errorf("failed to load %s from db: %w", hex.EncodeToString(iter.Key()[5:]), err)
		}

		t := storage.NewTorrent(tr.Path, s, tr.Active)
		t.Info = tr.Info
		t.Header = tr.Header
		t.BagID = tr.BagID

		if t.Info != nil {
			t.InitMask()
			// cache header
			err = t.BuildCache(int(t.Info.HeaderSize/uint64(t.Info.PieceSize)) + 1)
			if err != nil {
				return fmt.Errorf("failed to build cache for %s: %w", hex.EncodeToString(t.BagID), err)
			}
		}

		err = s.addTorrent(t)
		if err != nil {
			return fmt.Errorf("failed to add torrent %s from db: %w", hex.EncodeToString(t.BagID), err)
		}
	}

	return nil
}

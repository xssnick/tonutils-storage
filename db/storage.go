package db

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-storage/storage"
	"io"
	"os"
	"sort"
	"sync"
	"time"
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
	connector       storage.NetConnector

	db *leveldb.DB
	mx sync.RWMutex
}

func NewStorage(db *leveldb.DB, connector storage.NetConnector, startWithoutActiveFilesToo bool) (*Storage, error) {
	s := &Storage{
		torrents:        map[string]*storage.Torrent{},
		torrentsOverlay: map[string]*storage.Torrent{},
		db:              db,
		connector:       connector,
	}

	err := s.loadTorrents(startWithoutActiveFilesToo)
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
	sort.Slice(res, func(i, j int) bool {
		return res[i].CreatedAt.Unix() > res[j].CreatedAt.Unix()
	})
	return res
}

func (s *Storage) SetSpeedLimits(download, upload uint64) error {
	k := make([]byte, 13)
	copy(k, "speed_limits:")

	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data, download)
	binary.LittleEndian.PutUint64(data[8:], upload)

	return s.db.Put(k, data, nil)
}

func (s *Storage) GetSpeedLimits() (download uint64, upload uint64, err error) {
	k := make([]byte, 13)
	copy(k, "speed_limits:")

	var data []byte
	data, err = s.db.Get(k, nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	return binary.LittleEndian.Uint64(data), binary.LittleEndian.Uint64(data[8:]), nil
}

func (s *Storage) RemoveTorrent(t *storage.Torrent, withFiles bool) error {
	id, err := adnl.ToKeyID(adnl.PublicKeyOverlay{Key: t.BagID})
	if err != nil {
		return err
	}
	s.mx.Lock()
	delete(s.torrents, string(t.BagID))
	delete(s.torrentsOverlay, string(id))
	s.mx.Unlock()

	t.Stop()

	k := make([]byte, 5+32)
	copy(k, "bags:")
	copy(k[5:], t.BagID)

	if err = s.db.Delete(k, nil); err != nil {
		return err
	}

	if t.Header != nil {
		if withFiles {
			list, err := t.ListFiles()
			if err == nil {
				for _, f := range list {
					_ = os.Remove(t.Path + "/" + string(t.Header.DirName) + "/" + f)
				}
			}

			if yes, _ := isDirEmpty(t.Path + "/" + string(t.Header.DirName)); yes {
				_ = os.Remove(t.Path + "/" + string(t.Header.DirName))
			}
		}

		for i := uint32(0); i < t.Header.FilesCount; i++ {
			_ = s.RemoveFileIndex(t.BagID, i)
		}
	}

	if t.Info != nil {
		for i := uint32(0); i < t.PiecesNum(); i++ {
			_ = s.RemovePiece(t.BagID, i)
		}
	}
	return nil
}

func isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func (s *Storage) SetTorrent(t *storage.Torrent) error {
	activeDownload, activeUpload := t.IsActive()
	data, err := json.Marshal(&TorrentStored{
		BagID:          t.BagID,
		Path:           t.Path,
		Info:           t.Info,
		Header:         t.Header,
		CreatedAt:      t.CreatedAt,
		ActiveUpload:   activeUpload,
		ActiveDownload: activeDownload,
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
	BagID     []byte
	Path      string
	Info      *storage.TorrentInfo
	Header    *storage.TorrentHeader
	CreatedAt time.Time

	ActiveUpload   bool
	ActiveDownload bool
}

func (s *Storage) loadTorrents(startWithoutActiveFilesToo bool) error {
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

		t := storage.NewTorrent(tr.Path, s, s.connector)
		t.Info = tr.Info
		t.Header = tr.Header
		t.BagID = tr.BagID
		t.CreatedAt = tr.CreatedAt

		if t.Info != nil {
			t.InitMask()
			// cache header
			/*err = t.BuildCache(int(t.Info.HeaderSize/uint64(t.Info.PieceSize)) + 1)
			if err != nil {
				log.Printf("failed to build cache for %s: %s", hex.EncodeToString(t.BagID), err.Error())
				continue
			}*/
			_ = t.LoadActiveFilesIDs()
		}

		if tr.ActiveDownload {
			if startWithoutActiveFilesToo || len(t.GetActiveFilesIDs()) > 0 {
				err = t.Start(tr.ActiveUpload)
				if err != nil {
					return fmt.Errorf("failed to startd download %s: %w", hex.EncodeToString(iter.Key()[5:]), err)
				}
			}
		}

		err = s.addTorrent(t)
		if err != nil {
			return fmt.Errorf("failed to add torrent %s from db: %w", hex.EncodeToString(t.BagID), err)
		}
	}

	return nil
}

package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-storage/storage"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Event int

const (
	EventTorrentUpdated Event = iota
	EventUploadUpdated
)

type Storage struct {
	torrents        map[string]*storage.Torrent
	torrentsOverlay map[string]*storage.Torrent
	connector       storage.NetConnector
	fs              OsFs
	skipVerify      bool
	forcePieceSize  uint32

	notifyCh chan Event
	db       *leveldb.DB
	mx       sync.RWMutex
}

func NewStorage(db *leveldb.DB, connector storage.NetConnector, forcePieceSize int, startWithoutActiveFilesToo bool, skipVerify bool, noRemove bool, notifier chan Event) (*Storage, error) {
	if forcePieceSize < 0 {
		return nil, fmt.Errorf("invalid piece size flag")
	}

	s := &Storage{
		torrents:        map[string]*storage.Torrent{},
		torrentsOverlay: map[string]*storage.Torrent{},
		db:              db,
		connector:       connector,
		fs: OsFs{
			ctrl: NewFSControllerCache(noRemove),
		},
		notifyCh:       notifier,
		skipVerify:     skipVerify,
		forcePieceSize: uint32(forcePieceSize),
	}

	err := s.loadTorrents(startWithoutActiveFilesToo)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) VerifyOnStartup() bool {
	return !s.skipVerify
}

func (s *Storage) GetForcedPieceSize() uint32 {
	return s.forcePieceSize
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
	id, err := tl.Hash(adnl.PublicKeyOverlay{Key: t.BagID})
	if err != nil {
		return err
	}
	s.mx.Lock()
	delete(s.torrents, string(t.BagID))
	delete(s.torrentsOverlay, string(id))
	s.mx.Unlock()

	t.Stop()
	t.Wait()

	b := &leveldb.Batch{}
	b.Delete(append([]byte("bags:"), t.BagID...))
	b.Delete(append([]byte("upl_stat:"), t.BagID...))

	if err = s.db.Write(b, nil); err != nil {
		return err
	}

	if t.Header != nil {
		if withFiles {
			list, err := t.ListFiles()
			if err == nil {
				for _, f := range list {
					path := filepath.Clean(t.Path + "/" + string(t.Header.DirName) + "/" + f)
					if errR := s.fs.GetController().RemoveFile(path); errR != nil {
						println("remove err, skip", path, errR.Error())
					}
				}
			}
			recursiveEmptyDelete(buildTreeFromDir(t.Path+"/"+string(t.Header.DirName)), s.fs.GetController())
		}
	}

	if t.Info != nil {
		num := t.Info.PiecesNum()
		for i := uint32(0); i < num; i++ {
			_ = s.RemovePiece(t.BagID, i)
		}
	}
	s.notify(EventTorrentUpdated)
	return nil
}

func (s *Storage) SetTorrent(t *storage.Torrent) error {
	activeDownload, activeUpload := t.IsActiveRaw()
	data, err := json.Marshal(&TorrentStored{
		BagID:           t.BagID,
		Path:            t.Path,
		Info:            t.Info,
		Header:          t.Header,
		CreatedAt:       t.CreatedAt,
		ActiveUpload:    activeUpload,
		ActiveDownload:  activeDownload,
		DownloadAll:     t.IsDownloadAll(),
		DownloadOrdered: t.IsDownloadOrdered(),
		CreatedLocally:  t.CreatedLocally,
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
	id, err := tl.Hash(adnl.PublicKeyOverlay{Key: t.BagID})
	if err != nil {
		return err
	}

	s.mx.Lock()
	s.torrents[string(t.BagID)] = t
	s.torrentsOverlay[string(id)] = t
	s.mx.Unlock()
	s.notify(EventTorrentUpdated)
	return nil
}

type TorrentStored struct {
	BagID          []byte
	Path           string
	Info           *storage.TorrentInfo
	Header         *storage.TorrentHeader
	CreatedAt      time.Time
	CreatedLocally bool

	ActiveUpload    bool
	ActiveDownload  bool
	DownloadAll     bool
	DownloadOrdered bool
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
		t.CreatedLocally = tr.CreatedLocally

		uplStat, err := s.db.Get(append([]byte("upl_stat:"), t.BagID...), nil)
		if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
			return fmt.Errorf("failed to load upload stats of %s from db: %w", hex.EncodeToString(iter.Key()[5:]), err)
		}

		if t.Info != nil {
			t.InitMask()
			// cache header
			/*err = t.BuildCache(int(t.Info.HeaderSize/uint64(t.Info.PieceSize)) + 1)
			if err != nil {
				log.Printf("failed to build cache for %s: %s", hex.EncodeToString(t.BagID), err.Error())
				continue
			}*/
			_ = t.LoadActiveFilesIDs()

			if len(uplStat) == 8 {
				t.SetUploadStats(binary.LittleEndian.Uint64(uplStat))
			}
		}

		if tr.ActiveDownload {
			if startWithoutActiveFilesToo || len(t.GetActiveFilesIDs()) > 0 {
				err = t.Start(tr.ActiveUpload, tr.DownloadAll, tr.DownloadOrdered)
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

func (s *Storage) UpdateUploadStats(bagId []byte, val uint64) error {
	k := make([]byte, 9+32)
	copy(k, "upl_stat:")
	copy(k[9:], bagId)

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, val)

	if err := s.db.Put(k, data, nil); err != nil {
		return err
	}
	s.notify(EventUploadUpdated)
	return nil
}

func (s *Storage) notify(e Event) {
	if s.notifyCh != nil {
		select {
		case s.notifyCh <- e:
		default:
		}
	}
}

func (s *Storage) Close() {
	s.mx.Lock()
	defer s.mx.Unlock()

	for _, t := range s.torrents {
		t.Stop()
	}

	s.db.Close()
}

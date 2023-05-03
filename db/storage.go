package db

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-storage/torrent"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Config struct {
	Key           ed25519.PrivateKey
	ListenAddr    string
	ExternalIP    string
	DownloadsPath string
}

type Storage struct {
	dbPath          string
	torrents        map[string]*torrent.Torrent
	torrentsOverlay map[string]*torrent.Torrent
	mx              sync.RWMutex
}

func NewStorage(dbPath string) (*Storage, error) {
	s := &Storage{
		dbPath:          dbPath,
		torrents:        map[string]*torrent.Torrent{},
		torrentsOverlay: map[string]*torrent.Torrent{},
	}

	err := s.loadTorrents(dbPath + "/torrents")
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) GetTorrent(hash []byte) *torrent.Torrent {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.torrents[string(hash)]
}

func (s *Storage) GetTorrentByOverlay(overlay []byte) *torrent.Torrent {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.torrentsOverlay[string(overlay)]
}

func (s *Storage) GetAll() []*torrent.Torrent {
	s.mx.RLock()
	defer s.mx.RUnlock()

	res := make([]*torrent.Torrent, 0, len(s.torrents))
	for _, t := range s.torrents {
		res = append(res, t)
	}
	return res
}

func (s *Storage) AddTorrent(t *torrent.Torrent) error {
	f, err := os.Create(s.dbPath + "/torrents/" + hex.EncodeToString(t.BagID) + ".json")
	if err != nil {
		return err
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(t)
	if err != nil {
		return err
	}

	return s.addTorrent(t)
}

func (s *Storage) addTorrent(t *torrent.Torrent) error {
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

func (s *Storage) loadTorrents(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(path, os.ModePerm)
		}
		if err != nil {
			return err
		}
	}

	err = filepath.Walk(path, func(filePath string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		if err != nil {
			log.Println("failed to load", info.Name())
			return nil
		}

		file, err := os.Open(filePath)
		if err != nil {
			log.Println("failed to open", info.Name())
			return nil
		}
		defer file.Close()

		var t *torrent.Torrent
		if err = json.NewDecoder(file).Decode(&t); err != nil {
			log.Println("failed to parse db for", info.Name())
			return nil
		}

		err = s.addTorrent(t)
		if err != nil {
			log.Println("failed to add", info.Name())
			return nil
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to load torrents: %w", err)
	}
	return nil
}

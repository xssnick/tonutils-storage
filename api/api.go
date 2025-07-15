package api

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"github.com/pterm/pterm"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/storage"
	"math/bits"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
)

type Error struct {
	Error string `json:"error"`
}

type Ok struct {
	Ok bool `json:"ok"`
}

type ADNLProofResponse struct {
	Key       []byte `json:"key"`
	Signature []byte `json:"signature"`
}

type ProofResponse struct {
	Proof []byte `json:"proof"`
}

type File struct {
	Index uint32 `json:"index"`
	Name  string `json:"name"`
	Size  uint64 `json:"size"`
}

type Peer struct {
	Addr          string `json:"addr"`
	ID            string `json:"id"`
	UploadSpeed   uint64 `json:"upload_speed"`
	DownloadSpeed uint64 `json:"download_speed"`
}

type BagDetailed struct {
	Bag
	BagPiecesNum  uint32 `json:"bag_pieces_num"`
	HasPiecesMask []byte `json:"has_pieces_mask"`
	Files         []File `json:"files"`
	Peers         []Peer `json:"peers"`

	PieceSize  uint32 `json:"piece_size"`
	BagSize    uint64 `json:"bag_size"`
	MerkleHash string `json:"merkle_hash"`
	Path       string `json:"path"`
}

type Bag struct {
	BagID         string `json:"bag_id"`
	Description   string `json:"description"`
	Downloaded    uint64 `json:"downloaded"`
	Size          uint64 `json:"size"`
	HeaderSize    uint64 `json:"header_size"`
	Peers         uint64 `json:"peers"`
	DownloadSpeed uint64 `json:"download_speed"`
	UploadSpeed   uint64 `json:"upload_speed"`
	FilesCount    uint64 `json:"files_count"`
	DirName       string `json:"dir_name"`
	Completed     bool   `json:"completed"`
	HeaderLoaded  bool   `json:"header_loaded"`
	InfoLoaded    bool   `json:"info_loaded"`
	Active        bool   `json:"active"`
	Seeding       bool   `json:"seeding"`
}

type List struct {
	Bags []Bag `json:"bags"`
}

type Created struct {
	BagID string `json:"bag_id"`
}

type Credentials struct {
	Login    string
	Password string
}

type Server struct {
	credentials   *Credentials
	connector     storage.NetConnector
	store         *db.Storage
	downloadsPath string
}

func NewServer(connector storage.NetConnector, store *db.Storage, downloadsPath string) *Server {
	return &Server{
		connector:     connector,
		store:         store,
		downloadsPath: downloadsPath,
	}
}

func (s *Server) SetCredentials(credentials *Credentials) {
	s.credentials = credentials
}

func (s *Server) Start(addr string) error {
	m := http.NewServeMux()
	m.HandleFunc("/api/v1/details", s.withAuth(s.handleDetails))
	m.HandleFunc("/api/v1/add", s.withAuth(s.handleAdd))
	m.HandleFunc("/api/v1/create", s.withAuth(s.handleCreate))
	m.HandleFunc("/api/v1/remove", s.withAuth(s.handleRemove))
	m.HandleFunc("/api/v1/stop", s.withAuth(s.handleStop))
	m.HandleFunc("/api/v1/list", s.withAuth(s.handleList))
	m.HandleFunc("/api/v1/piece/proof", s.withAuth(s.handlePieceProof))
	m.HandleFunc("/api/v1/sign/provider", s.withAuth(s.handleSignProvider))

	return http.ListenAndServe(addr, m)
}

func (s *Server) handleAdd(w http.ResponseWriter, r *http.Request) {
	req := struct {
		BagID       string   `json:"bag_id"`
		Path        string   `json:"path"`
		DownloadAll bool     `json:"download_all"`
		Files       []uint32 `json:"files"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response(w, http.StatusBadRequest, Error{err.Error()})
		return
	}

	bag, err := hex.DecodeString(req.BagID)
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}
	if len(bag) != 32 {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}

	tor := s.store.GetTorrent(bag)
	if tor == nil {
		path := req.Path
		if path == "" {
			path = s.downloadsPath
		}
		tor = storage.NewTorrent(filepath.Join(path, hex.EncodeToString(bag)), s.store, s.connector)
		tor.BagID = bag

		if err = tor.Start(true, req.DownloadAll, false); err != nil {
			pterm.Error.Println("Failed to start:", err.Error())
			response(w, http.StatusInternalServerError, Error{"Failed to start download:" + err.Error()})
			return
		}

		err = s.store.SetTorrent(tor)
		if err != nil {
			pterm.Error.Println("Failed to set storage:", err.Error())
			response(w, http.StatusInternalServerError, Error{"Failed to save to db:" + err.Error()})
			return
		}
		pterm.Success.Println("Bag added", hex.EncodeToString(bag))
	} else {
		if err = tor.Start(true, req.DownloadAll, false); err != nil {
			pterm.Error.Println("Failed to start:", err.Error())
			response(w, http.StatusInternalServerError, Error{"Failed to start download:" + err.Error()})
			return
		}
		pterm.Success.Println("Bag state updated", hex.EncodeToString(bag), "download all:", req.DownloadAll)
	}

	if len(req.Files) > 0 {
		if err = tor.SetActiveFilesIDs(req.Files); err != nil {
			pterm.Error.Println("Failed to set active files:", err.Error())
			response(w, http.StatusInternalServerError, Error{"Failed to set active files:" + err.Error()})
			return
		}
		pterm.Success.Println("Bag active files updated", hex.EncodeToString(bag))
	}

	response(w, http.StatusOK, Ok{true})
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	req := struct {
		Path          string   `json:"path"`
		Description   string   `json:"description"`
		KeepOnlyPaths []string `json:"keep_only_paths"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response(w, http.StatusBadRequest, Error{err.Error()})
		return
	}

	var order map[string]int
	var only map[string]bool
	if len(req.KeepOnlyPaths) > 0 {
		only = make(map[string]bool)
		order = make(map[string]int)
		for i, p := range req.KeepOnlyPaths {
			order[p] = i
			only[p] = true
		}
	}

	rootPath, dirName, files, err := s.store.DetectFileRefs(req.Path, only)
	if err != nil {
		pterm.Error.Println("Failed to read file refs:", err.Error())
		response(w, http.StatusInternalServerError, Error{err.Error()})
		return
	}

	if len(req.KeepOnlyPaths) > 0 {
		// Sort files based on the `order` map's order, ascending
		sort.Slice(files, func(i, j int) bool {
			iOrder, iExists := order[filepath.Join(rootPath, dirName, files[i].GetName())]
			jOrder, jExists := order[filepath.Join(rootPath, dirName, files[j].GetName())]

			if iExists && jExists {
				return iOrder < jOrder
			}
			// Files not in the `order` map should appear after ordered files
			if iExists {
				return true
			}
			if jExists {
				return false
			}
			// Default fallback comparison based on file names
			return files[i].GetName() < files[j].GetName()
		})
	}

	it, err := storage.CreateTorrent(r.Context(), rootPath, dirName, req.Description, s.store, s.connector, files, nil)
	if err != nil {
		pterm.Error.Println("Failed to create bag:", err.Error())
		response(w, http.StatusInternalServerError, Error{err.Error()})
		return
	}

	if err = it.Start(true, true, false); err != nil {
		pterm.Error.Println("Failed to start bag:", err.Error())
		response(w, http.StatusInternalServerError, Error{err.Error()})
		return
	}

	if err = s.store.SetTorrent(it); err != nil {
		pterm.Error.Println("Failed to save bag to db:", err.Error())
		response(w, http.StatusInternalServerError, Error{err.Error()})
		return
	}

	pterm.Success.Println("Bag created", hex.EncodeToString(it.BagID))
	response(w, http.StatusOK, Created{BagID: hex.EncodeToString(it.BagID)})
}

func (s *Server) handlePieceProof(w http.ResponseWriter, r *http.Request) {
	bag, err := hex.DecodeString(r.URL.Query().Get("bag_id"))
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}
	if len(bag) != 32 {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}

	piece, err := strconv.ParseUint(r.URL.Query().Get("piece"), 10, 32)
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid piece"})
		return
	}

	if tor := s.store.GetTorrent(bag); tor != nil {
		proof, err := tor.GetPieceProof(uint32(piece))
		if err == nil {
			response(w, http.StatusOK, ProofResponse{proof})
			return
		}
	}
	response(w, http.StatusNotFound, Ok{Ok: false})
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	req := struct {
		BagID     string `json:"bag_id"`
		WithFiles bool   `json:"with_files"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response(w, http.StatusBadRequest, Error{err.Error()})
		return
	}

	bag, err := hex.DecodeString(req.BagID)
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}
	if len(bag) != 32 {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}

	if tor := s.store.GetTorrent(bag); tor != nil {
		if err = s.store.RemoveTorrent(tor, req.WithFiles); err != nil {
			pterm.Error.Println("Failed to remove bag from db:", err.Error())
			response(w, http.StatusInternalServerError, Error{err.Error()})
			return
		}
		pterm.Success.Println("Bag removed", hex.EncodeToString(tor.BagID))
		response(w, http.StatusOK, Ok{Ok: true})
		return
	}
	response(w, http.StatusNotFound, Ok{Ok: false})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	var bags []Bag
	for _, t := range s.store.GetAll() {
		bags = append(bags, s.getBag(t, true).Bag)
	}
	response(w, http.StatusOK, List{Bags: bags})
}

func (s *Server) handleSignProvider(w http.ResponseWriter, r *http.Request) {
	req := struct {
		ProviderID string `json:"provider_id"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response(w, http.StatusBadRequest, Error{err.Error()})
		return
	}

	providerId, err := hex.DecodeString(req.ProviderID)
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid provider id"})
		return
	}

	if len(providerId) != 32 {
		response(w, http.StatusBadRequest, Error{"Invalid provider id"})
		return
	}

	res, err := tl.Serialize(transport.ADNLProofScheme{
		Key: providerId,
	}, true)
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid provider id, cannot serialize scheme"})
		return
	}

	key := s.connector.GetADNLPrivateKey()
	response(w, http.StatusOK, ADNLProofResponse{
		Key:       key.Public().(ed25519.PublicKey),
		Signature: ed25519.Sign(key, res),
	})
}

func (s *Server) handleDetails(w http.ResponseWriter, r *http.Request) {
	bag, err := hex.DecodeString(r.URL.Query().Get("bag_id"))
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}
	if len(bag) != 32 {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}

	if tor := s.store.GetTorrent(bag); tor != nil {
		response(w, http.StatusOK, s.getBag(tor, false))
		return
	}
	response(w, http.StatusNotFound, Ok{Ok: false})
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	req := struct {
		BagID string `json:"bag_id"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response(w, http.StatusBadRequest, Error{err.Error()})
		return
	}

	bag, err := hex.DecodeString(req.BagID)
	if err != nil {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}
	if len(bag) != 32 {
		response(w, http.StatusBadRequest, Error{"Invalid bag id"})
		return
	}

	if tor := s.store.GetTorrent(bag); tor != nil {
		tor.Stop()
		response(w, http.StatusOK, Ok{Ok: true})
		return
	}
	response(w, http.StatusNotFound, Ok{Ok: false})

}

func (s *Server) withAuth(next func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if crs := s.credentials; crs != nil {
			login, password, ok := r.BasicAuth()
			if !ok || login != crs.Login || password != crs.Password {
				response(w, http.StatusUnauthorized, Error{
					"Invalid credentials",
				})
				return
			}
		}
		next(w, r)
	}
}

func response(w http.ResponseWriter, status int, result any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

func (s *Server) getBag(t *storage.Torrent, short bool) BagDetailed {
	res := BagDetailed{
		Files: []File{},
		Peers: []Peer{},
	}

	var dow, upl, num uint64
	for id, p := range t.GetPeers() {
		dow += p.GetDownloadSpeed()
		upl += p.GetUploadSpeed()
		num++

		if !short {
			res.Peers = append(res.Peers, Peer{
				Addr:          p.Addr,
				ID:            id,
				UploadSpeed:   p.GetUploadSpeed(),
				DownloadSpeed: p.GetDownloadSpeed(),
			})
		}
	}

	var desc, dirName string
	var headerSz, full, downloaded, filesCount uint64
	completed, infoLoaded, headerLoaded := false, false, false
	if t.Info != nil {
		infoLoaded = true
		mask := t.PiecesMask()
		downloadedPieces := 0
		for _, b := range mask {
			downloadedPieces += bits.OnesCount8(b)
		}

		downloaded = uint64(downloadedPieces*int(t.Info.PieceSize)) - t.Info.HeaderSize
		if uint64(downloadedPieces*int(t.Info.PieceSize)) < t.Info.HeaderSize { // 0 if header not fully downloaded
			downloaded = 0
		}

		headerSz = t.Info.HeaderSize
		full = t.Info.FileSize - t.Info.HeaderSize
		if downloaded > full { // cut not full last piece
			downloaded = full
		}
		completed = uint32(downloadedPieces) == t.Info.PiecesNum()

		if !completed && !t.IsDownloadAll() && len(mask) > 0 {
			completed = true

			var wantSz uint64
			files := t.GetActiveFilesIDs()

			for _, f := range files {
				off, err := t.GetFileOffsetsByID(f)
				if err == nil {
					wantSz += off.Size

					if completed {
						for pc := off.FromPiece; pc <= off.ToPiece; pc++ {
							if mask[pc/8]&(1<<(pc%8)) == 0 {
								completed = false
								break
							}
						}
					}
				}
			}

			// TODO: better calc for not all files
			if downloaded > wantSz { // cut not full last piece
				downloaded = wantSz
			}
		}

		if !short {
			res.BagPiecesNum = t.Info.PiecesNum()
			res.HasPiecesMask = mask
		}

		desc = t.Info.Description.Value
		if t.Header != nil {
			headerLoaded = true
			dirName = string(t.Header.DirName)
			filesCount = uint64(t.Header.FilesCount)

			if !short {
				list, err := t.ListFiles()
				if err == nil {
					for _, fl := range list {
						fi, err := t.GetFileOffsets(fl)
						if err != nil {
							continue
						}

						res.Files = append(res.Files, File{
							Index: fi.Index,
							Name:  fi.Name,
							Size:  fi.Size,
						})
					}
				}
			}
		}

		res.BagSize = t.Info.FileSize
		res.PieceSize = t.Info.PieceSize
		res.MerkleHash = hex.EncodeToString(t.Info.RootHash)
	}

	res.Path = t.Path
	active, seeding := t.IsActive()
	res.Bag = Bag{
		BagID:         hex.EncodeToString(t.BagID),
		Description:   desc,
		Downloaded:    downloaded,
		Size:          full,
		HeaderSize:    headerSz,
		Peers:         num,
		DownloadSpeed: dow,
		UploadSpeed:   upl,
		FilesCount:    filesCount,
		DirName:       dirName,
		Completed:     completed,
		HeaderLoaded:  headerLoaded,
		InfoLoaded:    infoLoaded,
		Active:        active,
		Seeding:       seeding,
	}

	return res
}

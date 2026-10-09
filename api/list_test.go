package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	levelstorage "github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/xssnick/tonutils-storage/db"
	"github.com/xssnick/tonutils-storage/storage"
)

func listServer(t *testing.T, bags int) *Server {
	t.Helper()
	ldb, err := leveldb.Open(levelstorage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := db.NewStorage(ldb, nil, 0, false, true, false, nil)
	if err != nil {
		_ = ldb.Close()
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	for i := range bags {
		torrent := storage.NewTorrent("/test", store, nil)
		torrent.BagID = bytes.Repeat([]byte{byte(i)}, 32)
		torrent.CreatedAt = time.Unix(int64(i), 0)
		if err := store.SetTorrent(torrent); err != nil {
			t.Fatal(err)
		}
	}
	return NewServer(nil, store, "")
}

func TestListPagination(t *testing.T) {
	server := listServer(t, 5)
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		name, query string
		want        []int
	}{
		{"legacy", "", []int{4, 3, 2, 1, 0}},
		{"limit", "?limit=2", []int{4, 3}},
		{"offset", "?offset=2", []int{2, 1, 0}},
		{"page", "?limit=2&offset=2", []int{2, 1}},
		{"last page", "?limit=2&offset=4", []int{0}},
		{"large limit", "?limit=100&offset=1", []int{3, 2, 1, 0}},
		{"offset at end", "?offset=5", []int{}},
		{"offset past end", "?offset=99&limit=2", []int{}},
		{"maximum offset", fmt.Sprintf("?offset=%d&limit=%d", maxInt, maxInt), []int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.handleList(response, httptest.NewRequest(http.MethodGet, "/api/v1/list"+tc.query, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("status %d: %s", response.Code, response.Body)
			}
			var list List
			if err := json.Unmarshal(response.Body.Bytes(), &list); err != nil {
				t.Fatal(err)
			}
			want := make([]string, len(tc.want))
			for i, id := range tc.want {
				want[i] = hex.EncodeToString(bytes.Repeat([]byte{byte(id)}, 32))
			}
			got := make([]string, len(list.Bags))
			for i, bag := range list.Bags {
				got[i] = bag.BagID
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("bags %v, want %v", got, want)
			}
			if list.Bags == nil {
				t.Fatal("an empty page should return an empty array")
			}
		})
	}
}

func TestListRejectsInvalidPagination(t *testing.T) {
	server := listServer(t, 1)
	for _, tc := range []struct{ name, query string }{
		{"zero limit", "?limit=0"},
		{"negative limit", "?limit=-1"},
		{"empty limit", "?limit="},
		{"noninteger limit", "?limit=1.5"},
		{"overflow limit", "?limit=999999999999999999999999"},
		{"negative offset", "?offset=-1"},
		{"empty offset", "?offset="},
		{"invalid offset", "?offset=no"},
		{"overflow offset", "?offset=999999999999999999999999"},
		{"duplicate limit", "?limit=1&limit=2"},
		{"duplicate offset", "?offset=1&offset=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.handleList(response, httptest.NewRequest(http.MethodGet, "/api/v1/list"+tc.query, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status %d: %s", response.Code, response.Body)
			}
		})
	}
}

func TestListPreservesLegacyEmptyResponse(t *testing.T) {
	server := listServer(t, 0)
	for _, tc := range []struct{ name, query, want string }{
		{"legacy", "", `{"bags":null}`},
		{"page", "?limit=10", `{"bags":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.handleList(response, httptest.NewRequest(http.MethodGet, "/api/v1/list"+tc.query, nil))
			if got := strings.TrimSpace(response.Body.String()); got != tc.want {
				t.Fatalf("response %s, want %s", got, tc.want)
			}
		})
	}
}

type summaryStorage struct {
	storage.Storage
	mask   []byte
	active []uint32
}

func (s summaryStorage) PiecesMask([]byte, uint32) []byte { return s.mask }
func (s summaryStorage) GetActiveFiles([]byte) ([]uint32, error) {
	return append([]uint32(nil), s.active...), nil
}
func (s summaryStorage) VerifyOnStartup() bool                  { return false }
func (s summaryStorage) UpdateUploadStats([]byte, uint64) error { return nil }

func TestBagSummaryAndDetails(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mask        byte
		downloadAll bool
		wantDone    bool
		wantBytes   uint64
	}{
		{"complete", 0b1111, false, true, 20},
		{"selected complete", 0b0011, false, true, 8},
		{"selected missing piece", 0b0101, false, false, 8},
		{"download all incomplete", 0b0011, true, false, 8},
		{"header incomplete", 0, true, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := summaryStorage{mask: []byte{tc.mask}, active: []uint32{0}}
			torrent := storage.NewTorrent("/test", store, nil)
			torrent.BagID = make([]byte, 32)
			torrent.Info = &storage.TorrentInfo{PieceSize: 8, HeaderSize: 8, FileSize: 28}
			torrent.Header = &storage.TorrentHeader{
				FilesCount: 2, TotalNameSize: 2, Names: []byte("ab"),
				NameIndex: []uint64{1, 2}, DataIndex: []uint64{8, 20},
			}
			if err := torrent.InitMaskWithError(); err != nil {
				t.Fatal(err)
			}
			if err := torrent.LoadActiveFilesIDs(); err != nil {
				t.Fatal(err)
			}
			if tc.downloadAll {
				torrent.CreatedLocally = true
				if err := torrent.Start(false, true, false); err != nil {
					t.Fatal(err)
				}
				torrent.Stop()
				torrent.Wait()
			}
			server := &Server{}
			short := server.getBag(torrent, true)
			if short.Completed != tc.wantDone || short.Downloaded != tc.wantBytes || len(short.HasPiecesMask) != 0 {
				t.Fatalf("incorrect summary: %+v", short)
			}
			details := server.getBag(torrent, false)
			if !reflect.DeepEqual(details.Bag, short.Bag) || !bytes.Equal(details.HasPiecesMask, store.mask) || len(details.Files) != 2 {
				t.Fatalf("details differ from summary or omit files/mask: %+v", details)
			}
		})
	}
}

func TestListPaginationStableCreationOrder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		subsecond bool
		want      []int
	}{
		{"identical timestamps", false, []int{0, 1, 2, 3, 4}},
		{"same second", true, []int{4, 3, 2, 1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := listServer(t, 5)
			for _, torrent := range server.store.GetAll() {
				nanos := int64(0)
				if tc.subsecond {
					nanos = int64(torrent.BagID[0])
				}
				torrent.CreatedAt = time.Unix(123, nanos)
			}
			for range 10 {
				var got []int
				for offset := 0; offset < 5; offset += 2 {
					response := httptest.NewRecorder()
					server.handleList(response, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/list?limit=2&offset=%d", offset), nil))
					var page List
					if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
						t.Fatal(err)
					}
					for _, bag := range page.Bags {
						id, err := hex.DecodeString(bag.BagID)
						if err != nil {
							t.Fatal(err)
						}
						got = append(got, int(id[0]))
					}
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("pages returned %v, want %v", got, tc.want)
				}
			}
		})
	}
}

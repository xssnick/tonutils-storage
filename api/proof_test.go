package api

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-storage/storage"
)

func TestPieceProofErrors(t *testing.T) {
	t.Parallel()
	server := listServer(t, 1)
	bagID := strings.Repeat("00", 32)
	bag, err := hex.DecodeString(bagID)
	if err != nil {
		t.Fatal(err)
	}
	torrent := server.store.GetTorrent(bag)
	for _, tc := range []struct {
		name, query string
		info        *storage.TorrentInfo
		status      int
		body        string
	}{
		{"missing bag", "?piece=0", nil, http.StatusBadRequest, `{"error":"Invalid bag id"}`},
		{"invalid bag encoding", "?bag_id=zz&piece=0", nil, http.StatusBadRequest, `{"error":"Invalid bag id"}`},
		{"short bag", "?bag_id=00&piece=0", nil, http.StatusBadRequest, `{"error":"Invalid bag id"}`},
		{"missing piece", "?bag_id=" + bagID, nil, http.StatusBadRequest, `{"error":"Invalid piece"}`},
		{"negative piece", "?bag_id=" + bagID + "&piece=-1", nil, http.StatusBadRequest, `{"error":"Invalid piece"}`},
		{"noninteger piece", "?bag_id=" + bagID + "&piece=1.5", nil, http.StatusBadRequest, `{"error":"Invalid piece"}`},
		{"overflow piece", "?bag_id=" + bagID + "&piece=4294967296", nil, http.StatusBadRequest, `{"error":"Invalid piece"}`},
		{"unknown bag", "?bag_id=" + strings.Repeat("01", 32) + "&piece=0", nil, http.StatusNotFound, `{"ok":false}`},
		{"info not loaded", "?bag_id=" + bagID + "&piece=0", nil, http.StatusNotFound, `{"ok":false,"error":"torrent metadata is not loaded"}`},
		{"zero piece size", "?bag_id=" + bagID + "&piece=0", &storage.TorrentInfo{FileSize: 1}, http.StatusNotFound, `{"ok":false,"error":"torrent metadata is not loaded"}`},
		{"piece past end", "?bag_id=" + bagID + "&piece=1", &storage.TorrentInfo{PieceSize: 1, FileSize: 1}, http.StatusNotFound, `{"ok":false,"error":"piece 1 not found, pieces count: 1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			torrent.Info = tc.info
			response := httptest.NewRecorder()
			server.handlePieceProof(response, httptest.NewRequest(http.MethodGet, "/api/v1/piece/proof"+tc.query, nil))
			if response.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", response.Code, tc.status, response.Body)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type %q, want application/json", got)
			}
			var got, want map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.body), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response %s, want %s", response.Body, tc.body)
			}
		})
	}
}

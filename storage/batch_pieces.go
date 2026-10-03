package storage

import "sort"

// The optional batch interface preserves compatibility with custom stores.
func (t *Torrent) setPieces(pieces map[uint32]*PieceInfo) error {
	if len(pieces) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(pieces))
	for id := range pieces {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	batchStore, ok := t.db.(interface {
		SetPieces([]byte, map[uint32]*PieceInfo) error
	})
	if !ok {
		for _, id := range ids {
			if err := t.setPiece(id, pieces[id], false); err != nil {
				return err
			}
		}
		return nil
	}
	if err := batchStore.SetPieces(t.BagID, pieces); err != nil {
		return err
	}
	t.maskMx.Lock()
	t.ensureDownloadedPiecesCountLocked()
	for _, id := range ids {
		if !bitsetHas(t.pieceMask, id) {
			if bitsetSet(t.pieceMask, id) {
				t.downloadedPieces.Add(1)
			}
		}
	}
	t.maskMx.Unlock()
	for _, id := range ids {
		t.enqueueNewPiece(id)
	}
	select {
	case t.signalNewPieces <- struct{}{}:
	default:
	}
	return nil
}

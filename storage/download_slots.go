package storage

import "context"

func (s *Server) acquireDownloadSlot(ctx context.Context) error {
	for {
		s.downloadSlotsMu.Lock()
		if err := ctx.Err(); err != nil {
			s.downloadSlotsMu.Unlock()
			return err
		}
		if s.downloadInflight.Load() < s.downloadMaxInflight.Load() {
			s.downloadInflight.Add(1)
			s.downloadSlotsMu.Unlock()
			return nil
		}
		if s.downloadSlotsChanged == nil {
			s.downloadSlotsChanged = make(chan struct{})
		}
		changed := s.downloadSlotsChanged
		s.downloadSlotsMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *Server) releaseDownloadSlot() {
	s.downloadSlotsMu.Lock()
	s.downloadInflight.Add(-1)
	if s.downloadSlotsChanged != nil {
		close(s.downloadSlotsChanged)
		s.downloadSlotsChanged = nil
	}
	s.downloadSlotsMu.Unlock()
}

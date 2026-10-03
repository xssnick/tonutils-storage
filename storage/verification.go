package storage

import (
	"context"
	"time"
)

func (t *Torrent) runVerification(ctx context.Context, deep bool) (bool, error) {
	t.verificationMx.Lock()
	defer t.verificationMx.Unlock()

	t.mx.Lock()
	t.isVerificationInProgress = true
	if t.stopDownload != nil {
		t.stopDownload()
	}
	t.mx.Unlock()

	// Wait for every write, fsync and metadata commit from the old downloader.
	// Its replacement waits here too, and sees the verification flag at entry.
	t.dataMx.Lock()
	intact, err := t.verify(ctx, deep, false)
	t.dataMx.Unlock()

	t.mx.Lock()
	t.isVerificationInProgress = false
	if err == nil {
		t.lastVerified = time.Now()
	}
	if !t.CreatedLocally && t.globalCtx != nil && t.globalCtx.Err() == nil {
		// The planner also restores empty files and cached boundary ranges when
		// the mask already contains every piece. Use the latest file selection.
		if restartErr := t.startDownload(nil); err == nil && restartErr != nil {
			err = restartErr
		}
	}
	t.mx.Unlock()
	t.wake.fire()
	return intact, err
}

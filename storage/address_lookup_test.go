package storage

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/tl"
)

type addressLookupResult struct {
	addrs *address.List
	err   error
}

type addressLookupRequest struct {
	ctx      context.Context
	id       []byte
	response chan addressLookupResult
	finished chan struct{}
}

type concurrentAddressDHT struct {
	recordingDHT
	started               chan *addressLookupRequest
	successOnCancellation bool
	calls                 atomic.Int64
	active                atomic.Int64
	peak                  atomic.Int64
}

func newConcurrentAddressDHT() *concurrentAddressDHT {
	return &concurrentAddressDHT{started: make(chan *addressLookupRequest, 128)}
}

func (d *concurrentAddressDHT) FindAddresses(ctx context.Context, id []byte) (*address.List, ed25519.PublicKey, error) {
	d.calls.Add(1)
	active := d.active.Add(1)
	for peak := d.peak.Load(); active > peak; peak = d.peak.Load() {
		if d.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	request := &addressLookupRequest{
		ctx: ctx, id: append([]byte(nil), id...),
		response: make(chan addressLookupResult, 1), finished: make(chan struct{}),
	}
	defer func() {
		d.active.Add(-1)
		close(request.finished)
	}()
	select {
	case d.started <- request:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	select {
	case result := <-request.response:
		return result.addrs, nil, result.err
	case <-ctx.Done():
		if d.successOnCancellation {
			return testNodeAddresses(), nil, nil
		}
		return nil, nil, ctx.Err()
	}
}

func testNodeAddresses() *address.List {
	return &address.List{Addresses: []address.Address{address.UDP{IP: net.IPv4(127, 0, 0, 1), Port: 1234}}}
}

func startTestAddressLookup(ctx context.Context, server *Server, id []byte) <-chan addressLookupResult {
	done := make(chan addressLookupResult, 1)
	go func() {
		addrs, err := server.lookupNodeAddresses(ctx, id)
		done <- addressLookupResult{addrs: addrs, err: err}
	}()
	return done
}

func TestAddressLookupCoalescesConcurrentWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newConcurrentAddressDHT()
		server := &Server{dht: fake, dhtParallelism: 3, closeCtx: t.Context()}
		const count = 100
		results := make([]<-chan addressLookupResult, count)
		for i := range results {
			results[i] = startTestAddressLookup(t.Context(), server, []byte("same-node"))
		}
		synctest.Wait()
		if calls := fake.calls.Load(); calls != 1 {
			t.Fatalf("got %d FindAddresses calls for 100 waiters, want 1", calls)
		}
		server.dhtCacheMx.RLock()
		waiters := server.addressLookups[hex.EncodeToString([]byte("same-node"))].waiters
		server.dhtCacheMx.RUnlock()
		if waiters != count {
			t.Fatalf("got %d shared waiters, want %d", waiters, count)
		}
		request := <-fake.started
		if string(request.id) != "same-node" {
			t.Fatalf("FindAddresses got node id %q", request.id)
		}
		addrs := testNodeAddresses()
		request.response <- addressLookupResult{addrs: addrs}
		for _, done := range results {
			result := <-done
			if result.err != nil || result.addrs != addrs {
				t.Fatalf("shared waiter got addresses %p, error %v", result.addrs, result.err)
			}
		}
		if cached, err := server.lookupNodeAddresses(t.Context(), []byte("same-node")); err != nil || cached != addrs {
			t.Fatalf("cached lookup got addresses %p, error %v", cached, err)
		}
		if fake.calls.Load() != 1 {
			t.Fatal("cached result repeated FindAddresses")
		}
	})
}

func TestAddressLookupWaiterCancellation(t *testing.T) {
	t.Run("one waiter preserves another", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fake := newConcurrentAddressDHT()
			server := &Server{dht: fake, dhtParallelism: 1, closeCtx: t.Context()}
			ctx, cancel := context.WithCancel(t.Context())
			first := startTestAddressLookup(ctx, server, []byte("node"))
			second := startTestAddressLookup(t.Context(), server, []byte("node"))
			synctest.Wait()
			request := <-fake.started
			cancel()
			if result := <-first; !errors.Is(result.err, context.Canceled) {
				t.Fatalf("cancelled waiter got %v", result.err)
			}
			synctest.Wait()
			if err := request.ctx.Err(); err != nil {
				t.Fatalf("one waiter cancelled the shared lookup: %v", err)
			}
			addrs := testNodeAddresses()
			request.response <- addressLookupResult{addrs: addrs}
			if result := <-second; result.err != nil || result.addrs != addrs {
				t.Fatalf("remaining waiter got addresses %p, error %v", result.addrs, result.err)
			}
			if fake.calls.Load() != 1 {
				t.Fatal("waiter cancellation caused an extra lookup")
			}
		})
	})
	t.Run("last waiter cancels lookup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fake := newConcurrentAddressDHT()
			server := &Server{dht: fake, dhtParallelism: 1, closeCtx: t.Context()}
			ctx, cancel := context.WithCancel(t.Context())
			done := startTestAddressLookup(ctx, server, []byte("node"))
			request := <-fake.started
			cancel()
			if result := <-done; !errors.Is(result.err, context.Canceled) {
				t.Fatalf("last waiter got %v", result.err)
			}
			<-request.finished
			synctest.Wait()
			if !errors.Is(request.ctx.Err(), context.Canceled) || fake.active.Load() != 0 {
				t.Fatal("lookup continues after its last waiter cancelled")
			}
			server.dhtCacheMx.RLock()
			pending, cached := len(server.addressLookups), len(server.dhtCache)
			server.dhtCacheMx.RUnlock()
			if pending != 0 || cached != 0 || len(server.findSemaphore) != 0 {
				t.Fatalf("cancelled lookup retained pending=%d, cached=%d, slots=%d", pending, cached, len(server.findSemaphore))
			}
			fresh := startTestAddressLookup(t.Context(), server, []byte("node"))
			retry := <-fake.started
			retry.response <- addressLookupResult{addrs: testNodeAddresses()}
			if result := <-fresh; result.err != nil {
				t.Fatalf("lookup after cancellation failed: %v", result.err)
			}
			if fake.calls.Load() != 2 {
				t.Fatal("retry did not start a new lookup")
			}
		})
	})
}

func TestAddressLookupReleasesSlotWhileSessionAdmissionIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newConcurrentAddressDHT()
		server := &Server{dht: fake, dhtParallelism: 1, closeCtx: t.Context()}
		releaseSession, err := server.acquireSessionSlot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer releaseSession()
		first := startTestAddressLookup(t.Context(), server, []byte("first"))
		firstRequest := <-fake.started
		second := startTestAddressLookup(t.Context(), server, []byte("second"))
		synctest.Wait()
		if fake.calls.Load() != 1 {
			t.Fatal("address lookup exceeded its independent capacity")
		}
		firstRequest.response <- addressLookupResult{addrs: testNodeAddresses()}
		if result := <-first; result.err != nil {
			t.Fatal(result.err)
		}
		secondRequest := <-fake.started
		if len(server.sessionSemaphore) != 1 {
			t.Fatal("address result released the occupied session slot")
		}
		secondRequest.response <- addressLookupResult{addrs: testNodeAddresses()}
		if result := <-second; result.err != nil {
			t.Fatal(result.err)
		}
		if len(server.findSemaphore) != 0 {
			t.Fatal("completed address lookup retained its slot")
		}
	})
}

func TestAddressLookupBoundsDistinctNodeConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newConcurrentAddressDHT()
		const count, parallelism = 20, 3
		server := &Server{dht: fake, dhtParallelism: parallelism, closeCtx: t.Context()}
		results := make([]<-chan addressLookupResult, count)
		for i := range results {
			results[i] = startTestAddressLookup(t.Context(), server, []byte(fmt.Sprintf("node-%d", i)))
		}
		synctest.Wait()
		if calls, peak := fake.calls.Load(), fake.peak.Load(); calls != parallelism || peak != parallelism {
			t.Fatalf("before release: calls=%d peak=%d, want %d", calls, peak, parallelism)
		}
		seen := make(map[string]bool, count)
		for range count {
			request := <-fake.started
			if id := string(request.id); seen[id] {
				t.Fatalf("distinct node lookup repeated id %q", id)
			} else {
				seen[id] = true
			}
			request.response <- addressLookupResult{addrs: testNodeAddresses()}
		}
		for _, done := range results {
			if result := <-done; result.err != nil {
				t.Fatal(result.err)
			}
		}
		if calls, peak := fake.calls.Load(), fake.peak.Load(); calls != count || peak != parallelism {
			t.Fatalf("completed: calls=%d peak=%d, want calls=%d peak=%d", calls, peak, count, parallelism)
		}
	})
}

func TestAddressCacheBoundsEntries(t *testing.T) {
	server := &Server{}
	now := time.Now()
	addrs := testNodeAddresses()
	server.dhtCacheMx.Lock()
	defer server.dhtCacheMx.Unlock()
	for i := 0; i < dhtAddressCacheLimit+3; i++ {
		server.cacheNodeAddressesLocked(fmt.Sprintf("node-%d", i), addrs, now)
		if len(server.dhtCache) > dhtAddressCacheLimit {
			t.Fatalf("cache exceeded %d entries after insert %d", dhtAddressCacheLimit, i)
		}
	}
	if len(server.dhtCache) != 8192 || server.dhtCacheOrder.Len() != 8192 {
		t.Fatalf("cache map/list lengths are %d/%d, want 8192", len(server.dhtCache), server.dhtCacheOrder.Len())
	}
	for i := range 3 {
		if server.dhtCache[fmt.Sprintf("node-%d", i)] != nil {
			t.Fatalf("oldest entry node-%d was not evicted", i)
		}
	}
	server.cacheNodeAddressesLocked("node-3", addrs, now.Add(time.Second))
	if len(server.dhtCache) != 8192 || server.dhtCacheOrder.Len() != 8192 {
		t.Fatal("refresh added another list entry or evicted an unrelated entry")
	}
}

func TestAddressCachePrunesExpiredEntriesAfterRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newConcurrentAddressDHT()
		server := &Server{dht: fake, dhtParallelism: 1, closeCtx: t.Context()}
		first, second := []byte("first"), []byte("second")
		old, fresh := testNodeAddresses(), testNodeAddresses()
		cache := func(id []byte, addrs *address.List) {
			server.dhtCacheMx.Lock()
			server.cacheNodeAddressesLocked(hex.EncodeToString(id), addrs, time.Now())
			server.dhtCacheMx.Unlock()
		}
		cache(first, old)
		cache(second, old)
		time.Sleep(dhtAddressCacheTTL / 2)
		cache(first, fresh)
		time.Sleep(dhtAddressCacheTTL / 2)
		if got, err := server.lookupNodeAddresses(t.Context(), first); err != nil || got != fresh {
			t.Fatalf("refreshed entry was not cached: %p, %v", got, err)
		}
		server.dhtCacheMx.RLock()
		count, order := len(server.dhtCache), server.dhtCacheOrder.Len()
		expired := server.dhtCache[hex.EncodeToString(second)]
		server.dhtCacheMx.RUnlock()
		if count != 1 || order != 1 || expired != nil || fake.calls.Load() != 0 {
			t.Fatalf("expired entry survived pruning: count=%d order=%d expired=%v calls=%d", count, order, expired != nil, fake.calls.Load())
		}
		time.Sleep(dhtAddressCacheTTL / 2)
		done := startTestAddressLookup(t.Context(), server, first)
		request := <-fake.started
		server.dhtCacheMx.RLock()
		count, order = len(server.dhtCache), server.dhtCacheOrder.Len()
		server.dhtCacheMx.RUnlock()
		if count != 0 || order != 0 {
			t.Fatal("expired refreshed entry was retained")
		}
		request.response <- addressLookupResult{addrs: testNodeAddresses()}
		if result := <-done; result.err != nil {
			t.Fatal(result.err)
		}
	})
}

func TestInitializeSessionWaitsForAdmissionAndReleasesSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &Server{dhtParallelism: 1}
		release, err := server.acquireSessionSlot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		peer, transport := newInitSnapshotTestPeer([]byte{1})
		peer.conn.srv = server
		peer.torrent.wake = newWakeSig()
		attempt := peer.currentSessionAttempt(t.Context())
		entered, finish := make(chan struct{}), make(chan struct{})
		transport.onQuery = func(tl.Serializable, tl.Serializable) error {
			close(entered)
			<-finish
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- peer.initializeSession(&attempt, false) }()
		synctest.Wait()
		select {
		case <-entered:
			t.Fatal("session sent init before admission")
		default:
		}
		release()
		<-entered
		if len(server.sessionSemaphore) != 1 {
			t.Fatal("in-progress init did not hold its session slot")
		}
		close(finish)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !peer.isLocalInitSent() || len(server.sessionSemaphore) != 0 {
			t.Fatal("completed init did not publish readiness and release its session slot")
		}
	})
}

func TestSessionAdmissionCancellationAndShutdown(t *testing.T) {
	for _, name := range []string{"caller cancellation", "server shutdown"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				serverCtx, stopServer := context.WithCancel(t.Context())
				defer stopServer()
				server := &Server{dhtParallelism: 1, closeCtx: serverCtx}
				release, err := server.acquireSessionSlot(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				peer, transport := newInitSnapshotTestPeer([]byte{1})
				peer.conn.srv = server
				peer.torrent.wake = newWakeSig()
				var queries atomic.Int64
				transport.onQuery = func(tl.Serializable, tl.Serializable) error {
					queries.Add(1)
					return nil
				}
				callerCtx, cancelCaller := context.WithCancel(t.Context())
				defer cancelCaller()
				attempt := peer.currentSessionAttempt(callerCtx)
				done := make(chan error, 1)
				go func() { done <- peer.initializeSession(&attempt, false) }()
				synctest.Wait()
				if name == "server shutdown" {
					stopServer()
				} else {
					cancelCaller()
				}
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("queued init returned %v, want cancellation", err)
				}
				if queries.Load() != 0 || len(server.sessionSemaphore) != 1 {
					t.Fatal("cancelled waiter sent a query or released another session's slot")
				}
				release()
				retryRelease, err := server.acquireSessionSlot(t.Context())
				if name == "server shutdown" {
					if !errors.Is(err, context.Canceled) || retryRelease != nil || len(server.sessionSemaphore) != 0 {
						t.Fatal("stopped server admitted another session or retained its slot")
					}
				} else {
					if err != nil {
						t.Fatalf("cancelled waiter prevented fresh session admission: %v", err)
					}
					retryRelease()
				}
			})
		})
	}
}

func TestAddressLookupRejectsSuccessfulResultAfterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := newConcurrentAddressDHT()
		fake.successOnCancellation = true
		serverCtx, stopServer := context.WithCancel(t.Context())
		server := &Server{dht: fake, dhtParallelism: 1, closeCtx: serverCtx}
		done := startTestAddressLookup(t.Context(), server, []byte("node"))
		request := <-fake.started
		stopServer()
		if result := <-done; !errors.Is(result.err, context.Canceled) {
			t.Fatalf("late successful result concealed lookup cancellation: %v", result.err)
		}
		<-request.finished
		server.dhtCacheMx.RLock()
		cached := len(server.dhtCache)
		server.dhtCacheMx.RUnlock()
		if cached != 0 || len(server.findSemaphore) != 0 {
			t.Fatal("late successful result was cached or retained its slot")
		}
	})
}

func TestSessionAdmissionStartsProgressTimeoutAfterQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &Server{dhtParallelism: 1}
		release, err := server.acquireSessionSlot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		peer, _ := newInitSnapshotTestPeer([]byte{1})
		peer.conn.srv = server
		peer.torrent.wake = newWakeSig()
		attempt := peer.currentSessionAttempt(t.Context())
		done := make(chan error, 1)
		go func() { done <- peer.initializeSession(&attempt, false) }()
		synctest.Wait()
		time.Sleep(55 * time.Second)
		release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if peer.initProgressTimedOut(time.Now(), 45*time.Second) {
			t.Fatal("55 seconds in admission queue exhausted the remote init progress timeout")
		}
		time.Sleep(46 * time.Second)
		if !peer.initProgressTimedOut(time.Now(), 45*time.Second) {
			t.Fatal("admission reset removed the remote init progress timeout")
		}
	})
}

func TestSessionAdmissionStaleAttemptCannotResetCurrentTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &Server{dhtParallelism: 1}
		release, err := server.acquireSessionSlot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		peer, transport := newInitSnapshotTestPeer([]byte{1})
		peer.conn.srv = server
		peer.torrent.wake = newWakeSig()
		// Keep the old caller context alive to exercise the generation guard even
		// when a stale caller does not use the cancelled session context.
		attempt := peer.currentSessionAttempt(t.Context())
		done := make(chan error, 1)
		go func() { done <- peer.initializeSession(&attempt, false) }()
		synctest.Wait()
		time.Sleep(55 * time.Second)
		newID := attempt.id + 1
		_, next := peer.torrent.prepareStoragePeer(peer.overlay, nil, peer.conn, &newID)
		if next == nil {
			t.Fatal("reinit did not create a new attempt")
		}
		currentStartedAt := atomic.LoadInt64(&peer.session.sessionInitAt)
		time.Sleep(5 * time.Second)
		release()
		if err := <-done; !errors.Is(err, errStaleSessionAttempt) {
			t.Fatalf("stale queued init returned %v", err)
		}
		if got := atomic.LoadInt64(&peer.session.sessionInitAt); got != currentStartedAt {
			t.Fatalf("stale queued init reset current session timestamp: %d -> %d", currentStartedAt, got)
		}
		if len(transport.queries) != 0 || len(server.sessionSemaphore) != 0 {
			t.Fatal("stale queued init sent a query or retained admission")
		}
	})
}

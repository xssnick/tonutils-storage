package storage

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/xssnick/tonutils-go/adnl/address"
)

const (
	dhtAddressCacheTTL      = 150 * time.Second
	dhtAddressCacheLimit    = 8192
	dhtAddressLookupTimeout = 90 * time.Second
)

// Waiters share one lookup, but cancelling one bag must not cancel the others.
type addressLookup struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	addrs   *address.List
	err     error
}

func (s *Server) lookupNodeAddresses(ctx context.Context, id []byte) (*address.List, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := hex.EncodeToString(id)
	s.dhtCacheMx.Lock()
	s.pruneAddressCacheLocked(time.Now())
	if entry := s.dhtCache[key]; entry != nil {
		s.dhtCacheMx.Unlock()
		return entry.addrs, nil
	}
	if s.addressLookups == nil {
		s.addressLookups = make(map[string]*addressLookup)
	}
	if s.findSemaphore == nil {
		s.findSemaphore = make(chan struct{}, max(1, s.dhtParallelism))
	}
	call := s.addressLookups[key]
	if call == nil {
		base := s.closeCtx
		if base == nil {
			base = context.Background()
		}
		lookupCtx, cancel := context.WithTimeout(base, dhtAddressLookupTimeout)
		call = &addressLookup{done: make(chan struct{}), cancel: cancel}
		s.addressLookups[key] = call
		go s.runAddressLookup(lookupCtx, key, append([]byte(nil), id...), call)
	}
	call.waiters++
	s.dhtCacheMx.Unlock()

	defer func() {
		s.dhtCacheMx.Lock()
		call.waiters--
		if call.waiters == 0 && s.addressLookups[key] == call {
			delete(s.addressLookups, key)
			call.cancel()
		}
		s.dhtCacheMx.Unlock()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return call.addrs, call.err
	}
}

func (s *Server) runAddressLookup(ctx context.Context, key string, id []byte, call *addressLookup) {
	defer call.cancel()
	var addrs *address.List
	var err error
	select {
	case s.findSemaphore <- struct{}{}:
		// The lookup slot is returned before any caller starts its session.
		if err = ctx.Err(); err == nil {
			addrs, _, err = s.dht.FindAddresses(ctx, id)
		}
		<-s.findSemaphore
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil {
		err = ctx.Err()
	}
	s.dhtCacheMx.Lock()
	if err == nil && ctx.Err() == nil && addrs != nil && len(addrs.Addresses) > 0 {
		s.cacheNodeAddressesLocked(key, addrs, time.Now())
	}
	call.addrs, call.err = addrs, err
	if s.addressLookups[key] == call {
		delete(s.addressLookups, key)
	}
	close(call.done)
	s.dhtCacheMx.Unlock()
}

func (s *Server) pruneAddressCacheLocked(now time.Time) {
	for front := s.dhtCacheOrder.Front(); front != nil; front = s.dhtCacheOrder.Front() {
		key := front.Value.(string)
		entry := s.dhtCache[key]
		if entry != nil && entry.expireAt.After(now) {
			break
		}
		delete(s.dhtCache, key)
		s.dhtCacheOrder.Remove(front)
	}
}

func (s *Server) cacheNodeAddressesLocked(key string, addrs *address.List, now time.Time) {
	s.pruneAddressCacheLocked(now)
	if s.dhtCache == nil {
		s.dhtCache = make(map[string]*dhtCacheEntry)
	}
	if old := s.dhtCache[key]; old != nil {
		s.dhtCacheOrder.Remove(old.order)
		delete(s.dhtCache, key)
	}
	for len(s.dhtCache) >= dhtAddressCacheLimit {
		front := s.dhtCacheOrder.Front()
		delete(s.dhtCache, front.Value.(string))
		s.dhtCacheOrder.Remove(front)
	}
	s.dhtCache[key] = &dhtCacheEntry{
		addrs: addrs, expireAt: now.Add(dhtAddressCacheTTL),
		order: s.dhtCacheOrder.PushBack(key),
	}
}

func (s *Server) acquireSessionSlot(ctx context.Context) (func(), error) {
	s.dhtCacheMx.Lock()
	if s.sessionSemaphore == nil {
		s.sessionSemaphore = make(chan struct{}, max(1, s.dhtParallelism))
	}
	sem := s.sessionSemaphore
	s.dhtCacheMx.Unlock()
	var stopped <-chan struct{}
	if s.closeCtx != nil {
		stopped = s.closeCtx.Done()
	}
	select {
	case sem <- struct{}{}:
		if s.closeCtx != nil && s.closeCtx.Err() != nil {
			<-sem
			return nil, s.closeCtx.Err()
		}
		if err := ctx.Err(); err != nil {
			<-sem
			return nil, err
		}
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-stopped:
		return nil, s.closeCtx.Err()
	}
}

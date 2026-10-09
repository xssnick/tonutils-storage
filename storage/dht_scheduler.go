package storage

import (
	"bytes"
	"container/heap"
	"sync"
	"time"
)

const (
	bagDHTInitialStoreDuration = 3 * time.Second
	bagDHTMaxSeedParallelism   = 512
)

type dhtJobClass uint8

const (
	dhtRegularJob dhtJobClass = iota
	dhtPriorityJob
	dhtSeedJob
)

// Seed refreshes have their own capacity, so a permanent download backlog
// cannot consume the slots needed to keep existing overlay records fresh.
type dhtJobPool struct {
	mx                           sync.Mutex
	regular, priority, seeds     int
	parallelism, seedParallelism int
	storeDuration                time.Duration
}

func newDHTJobPool(parallelism int) *dhtJobPool {
	return &dhtJobPool{
		parallelism:     max(1, parallelism),
		seedParallelism: max(1, parallelism),
		storeDuration:   bagDHTInitialStoreDuration,
	}
}

func bagDHTSeedParallelism(seedCount, minimum int, storeDuration time.Duration) int {
	// Include the scheduler tick and 25% headroom. A fixed pool of 20 cannot
	// refresh 5000 bags inside the freshness window when Store takes 3 seconds.
	cost := max(storeDuration, bagDHTInitialStoreDuration) + peerSearcherInterval
	required := (int64(seedCount)*int64(cost) + int64(bagDHTSeedRefreshInterval) - 1) / int64(bagDHTSeedRefreshInterval)
	required += (required + 3) / 4
	return min(bagDHTMaxSeedParallelism, max(1, minimum, int(required)))
}

func (p *dhtJobPool) resizeSeeds(seedCount int) int {
	p.mx.Lock()
	defer p.mx.Unlock()
	p.seedParallelism = bagDHTSeedParallelism(seedCount, p.parallelism, p.storeDuration)
	return p.seedParallelism
}

func (p *dhtJobPool) tryAcquire(class dhtJobClass) (func(time.Duration), bool) {
	p.mx.Lock()
	switch class {
	case dhtSeedJob:
		if p.seeds >= p.seedParallelism {
			p.mx.Unlock()
			return nil, false
		}
		p.seeds++
	default:
		if p.regular < p.parallelism {
			p.regular++
			class = dhtRegularJob
		} else if class == dhtPriorityJob && p.priority < p.parallelism {
			p.priority++
		} else {
			p.mx.Unlock()
			return nil, false
		}
	}
	p.mx.Unlock()
	return func(duration time.Duration) {
		p.mx.Lock()
		defer p.mx.Unlock()
		switch class {
		case dhtSeedJob:
			p.seeds--
			// React immediately to slow stores; decay gradually as they recover.
			if duration > p.storeDuration {
				p.storeDuration = duration
			} else {
				p.storeDuration = (p.storeDuration*7 + duration) / 8
			}
		case dhtPriorityJob:
			p.priority--
		default:
			p.regular--
		}
	}, true
}

type dhtSeedCandidate struct {
	torrent *Torrent
	dueAt   time.Time
}

type dhtSeedQueue []dhtSeedCandidate

func (q dhtSeedQueue) Len() int { return len(q) }
func (q dhtSeedQueue) Less(i, j int) bool {
	if !q[i].dueAt.Equal(q[j].dueAt) {
		return q[i].dueAt.Before(q[j].dueAt)
	}
	return bytes.Compare(q[i].torrent.BagID, q[j].torrent.BagID) < 0
}
func (q dhtSeedQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *dhtSeedQueue) Push(v any)   { *q = append(*q, v.(dhtSeedCandidate)) }
func (q *dhtSeedQueue) Pop() any {
	last := len(*q) - 1
	v := (*q)[last]
	(*q)[last] = dhtSeedCandidate{}
	*q = (*q)[:last]
	return v
}

func (q *dhtSeedQueue) next() dhtSeedCandidate { return heap.Pop(q).(dhtSeedCandidate) }

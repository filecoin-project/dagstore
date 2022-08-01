package gc

import (
	"sort"
	"time"

	"github.com/filecoin-project/dagstore/shard"
)

var _ GarbageCollectionStrategy = (*LRUGarbageCollector)(nil)

type shardMetadata struct {
	key            shard.Key
	reclaimable    bool
	lastAccessedAt time.Time
}

// LRUGarbageCollector implements a `Least Recently Used` strategy for
// determining the order in which the reclaimable shards it is tracking should be GC'd.
type LRUGarbageCollector struct {
	shards map[shard.Key]*shardMetadata
}

func NewLRUGarbageCollector() *LRUGarbageCollector {
	return &LRUGarbageCollector{
		shards: make(map[shard.Key]*shardMetadata),
	}
}

func (l *LRUGarbageCollector) NotifyReclaimable(key shard.Key) {
	l.updateF(key, func(sm *shardMetadata) { sm.reclaimable = true })
}

func (l *LRUGarbageCollector) NotifyNotReclaimable(key shard.Key) {
	l.updateF(key, func(sm *shardMetadata) { sm.reclaimable = false })
}

func (l *LRUGarbageCollector) NotifyAccessed(key shard.Key) {
	l.updateF(key, func(sm *shardMetadata) { sm.lastAccessedAt = time.Now() })
}

func (l *LRUGarbageCollector) updateF(key shard.Key, update func(sm *shardMetadata)) {
	sm, ok := l.shards[key]
	if !ok {
		sm = &shardMetadata{
			key: key,
		}
	}
	update(sm)
	l.shards[key] = sm
}

func (l *LRUGarbageCollector) NotifyRemoved(key shard.Key) {
	delete(l.shards, key)
}

func (l *LRUGarbageCollector) Reclaimable() []shard.Key {
	var reclaim []shardMetadata
	for _, s := range l.shards {
		sm := *s
		if s.reclaimable {
			reclaim = append(reclaim, sm)
		}
	}

	// Sort in LRU order
	sort.Slice(reclaim, func(i, j int) bool {
		return reclaim[i].lastAccessedAt.Before(reclaim[j].lastAccessedAt)
	})

	keys := make([]shard.Key, 0, len(reclaim))
	for _, s := range reclaim {
		keys = append(keys, s.key)
	}

	return keys
}

func (l *LRUGarbageCollector) NotifyReclaimed(keys []shard.Key) {
	for _, k := range keys {
		delete(l.shards, k)
	}
}

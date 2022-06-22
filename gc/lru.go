package gc

import (
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/dagstore/shard"
)

type shardAccess struct {
	k          shard.Key
	accessedAt time.Time
}

type LRUGarbageCollector struct {
	mu     sync.RWMutex
	access map[shard.Key]time.Time
}

func (l *LRUGarbageCollector) NotifyAccessed(k shard.Key) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.access[k] = time.Now()
}

func (l LRUGarbageCollector) Reclaimable() []shard.Key {
	l.mu.RLock()
	defer l.mu.RUnlock()

	reclaim := make([]shardAccess, 0, len(l.access))
	for s, t := range l.access {
		reclaim = append(reclaim, shardAccess{
			k:          s,
			accessedAt: t,
		})
	}

	sort.Slice(reclaim, func(i, j int) bool {
		return reclaim[i].accessedAt.Before(reclaim[j].accessedAt)
	})

	out := make([]shard.Key, 0, len(reclaim))
	for _, s := range reclaim {
		out = append(out, s.k)
	}
	return out
}

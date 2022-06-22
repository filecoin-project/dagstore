package gc

import "github.com/filecoin-project/dagstore/shard"

type GarbageCollector interface {
	NotifyAccessed(shard.Key)    // invoked every time an acquirer requests the shard
	NotifyReclaimable(shard.Key) // when shard goes back from Serving to Available; we could notify every time we release, but then the GarbageCollector would need to do usage tracking, which would be a duplication of responsibliities
	NotifyRemoved(shard.Key)     // stop tracking this shard, e.g. when a shard is terminally destroyed and the dagstore has already removed its resources
	Reclaimable() []shard.Key    // returns all shards ordered by reclaimability precedence.
	NotifyReclaimed([]shard.Key) // informs which shards were ultimately reclaimed.
}

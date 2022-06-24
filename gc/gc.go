package gc

import "github.com/filecoin-project/dagstore/shard"

type GarbageCollector interface {
	NotifyAccessed(shard.Key)
	NotifyReclaimable(shard.Key)
	NotifyNotReclaimable(key shard.Key)
	NotifyRemoved(shard.Key)
	Reclaimable() []shard.Key
	NotifyReclaimed([]shard.Key)
}

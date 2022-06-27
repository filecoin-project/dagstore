package gc

import "github.com/filecoin-project/dagstore/shard"

// GarbageCollectionStrategy manages the algorithm for deciding the order in which
// reclaimable transients should be garbage collected when the dagstore runs an automated GC.
// It is the dagstore's responsibility to inform the `GarbageCollectionStrategy` about which transients are reclaimable and
// which are not using the public interface of the `GarbageCollectionStrategy`.
//
// Note: The `GarbageCollectionStrategy` is only responsible for deciding the order in which reclaimable transients
// should be GC'd. The actual Garbage Collection is done by the dagstore which "owns" the transients.
//
// Implementations of `GarbageCollectionStrategy` are not meant to be thread safe and the dagstore provides thread safety here
// by only invoking the `GarbageCollectionStrategy` from the dagstore event loop.
// All methods of the `GarbageCollectionStrategy` should and will only be invoked from the dagstore's event loop.
type GarbageCollectionStrategy interface {
	// NotifyAccessed notifies the strategy when the shard with the given key is accessed for a read operation.
	NotifyAccessed(shard.Key)
	// NotifyReclaimable notifies the strategy that the shard with the given key is reclaimable.
	NotifyReclaimable(shard.Key)
	// NotifyNotReclaimable notifies the strategy that the shard with the given key is not reclaimable.
	NotifyNotReclaimable(key shard.Key)
	// NotifyRemoved notifies the strategy that the shard with the given key has been removed by the dagstore.
	NotifyRemoved(shard.Key)
	// NotifyReclaimed notifies the strategy that the shards with the given key have been reclaimed by the dagstore.
	// The dagstore will ideally call `Reclaimable` -> get an ordered list of shards that be GC'd and then call `NotifyReclaimed`
	// on the `GarbageCollectionStrategy` for all the shards that were actually GC'd/reclaimed.
	NotifyReclaimed([]shard.Key)
	// Reclaimable is called by the dagstore when it wants an ordered list of shards whose transients can
	// be reclaimed by GC. The shards are ordered from ins descending order of their eligiblity for GC.
	Reclaimable() []shard.Key
}

var _ GarbageCollectionStrategy = (*NoOpStrategy)(nil)

type NoOpStrategy struct{}

func (n *NoOpStrategy) NotifyAccessed(shard.Key)           {}
func (n *NoOpStrategy) NotifyReclaimable(shard.Key)        {}
func (n *NoOpStrategy) NotifyNotReclaimable(key shard.Key) {}
func (n *NoOpStrategy) NotifyRemoved(shard.Key)            {}
func (n *NoOpStrategy) NotifyReclaimed([]shard.Key)        {}
func (n *NoOpStrategy) Reclaimable() []shard.Key {
	return nil
}

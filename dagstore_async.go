package dagstore

import (
	"context"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"

	"github.com/filecoin-project/dagstore/mount"
)

//
// This file contains methods that are called from the event loop
// but are run asynchronously in dedicated goroutines.
//

// acquireAsync acquires a shard by fetching its data, obtaining its index, and
// joining them to form a ShardAccessor.
func (d *DAGStore) acquireAsync(ctx context.Context, w *waiter, s *Shard, mnt mount.Mount) {
	k := s.key

	reader, err := mnt.Fetch(ctx)

	if err := ctx.Err(); err != nil {
		log.Warnw("context cancelled while fetching shard; releasing", "shard", s.key, "error", err)

		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// send the shard error to the caller for correctness
		// since the context is cancelled, the result will be discarded.
		d.dispatchResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	if err != nil {
		log.Warnw("acquire: failed to fetch from mount upgrader", "shard", s.key, "error", err)

		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		// vyzo: swallowed error
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// fail the shard
		// vyzo: swallowed error
		_ = d.failShard(s, d.completionCh, "failed to acquire reader of mount so we can return the accessor: %w", err)

		// send the shard error to the caller.
		d.dispatchResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	log.Debugw("acquire: successfully fetched from mount upgrader", "shard", s.key)

	// acquire the index.
	idx, err := d.indices.GetFullIndex(k)

	if err := ctx.Err(); err != nil {
		log.Warnw("context cancelled while indexing shard; releasing", "shard", s.key, "error", err)

		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// send the shard error to the caller for correctness
		// since the context is cancelled, the result will be discarded.
		d.dispatchResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	if err != nil {
		log.Warnw("acquire: failed to get index for shard", "shard", s.key, "error", err)
		if err := reader.Close(); err != nil {
			log.Errorf("failed to close mount reader: %s", err)
		}

		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		// vyzo: swallowed error
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// fail the shard
		// vyzo: swallowed error
		_ = d.failShard(s, d.completionCh, "failed to recover index for shard %s: %w", k, err)

		// send the shard error to the caller.
		d.dispatchResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	log.Debugw("acquire: successful; returning accessor", "shard", s.key)

	// build the accessor.
	sa, err := NewShardAccessor(reader, idx, s)

	// send the shard accessor to the caller, adding a notifyDead function that
	// will be called to release the shard if we were unable to deliver
	// the accessor.
	w.notifyDead = func() {
		log.Warnw("context cancelled while delivering accessor; releasing", "shard", s.key)

		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)
	}

	d.dispatchResult(&ShardResult{Key: k, Accessor: sa, Error: err}, w)
}

// initializeShard initializes a shard asynchronously by fetching its data and
// performing indexing.
func (d *DAGStore) initializeShard(ctx context.Context, s *Shard, mnt mount.Mount) {
	reader, err := mnt.Fetch(ctx)
	if err != nil {
		log.Warnw("initialize: failed to fetch from mount upgrader", "shard", s.key, "error", err)

		_ = d.failShard(s, d.completionCh, "failed to acquire reader of mount on initialization: %w", err)
		return
	}
	defer reader.Close()

	log.Debugw("initialize: successfully fetched from mount upgrader", "shard", s.key)

	// works for both CARv1 and CARv2.
	var idx index.Index
	err = d.throttleIndex.Do(ctx, func(_ context.Context) error {
		var err error
		idx, err = car.ReadOrGenerateIndex(reader, car.ZeroLengthSectionAsEOF(true))
		if err == nil {
			log.Debugw("initialize: finished generating index for shard", "shard", s.key)
		} else {
			log.Warnw("initialize: failed to generate index for shard", "shard", s.key, "error", err)
		}
		return err
	})
	if err != nil {
		_ = d.failShard(s, d.completionCh, "failed to read/generate CAR Index: %w", err)
		return
	}
	if err := d.indices.AddFullIndex(s.key, idx); err != nil {
		_ = d.failShard(s, d.completionCh, "failed to add index for shard: %w", err)
		return
	}

	// vyzo: swallowed error
	_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
}

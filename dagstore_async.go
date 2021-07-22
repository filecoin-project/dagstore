package dagstore

import (
	"context"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
)

//
// This file contains methods that are called from the event loop
// but are run asynchronously in dedicated goroutines.
//

// acquireAsync acquires a shard by fetching its data, obtaining its index, and
// joining them to form a ShardAccessor.
func (d *DAGStore) acquireAsync(ctx context.Context, w *waiter, s *Shard, mnt mount.Mount) {
	k := s.key

	var reader mount.Reader
	err := d.throttleFetch.Do(ctx, func(ctx context.Context) error {
		var err error
		reader, err = mnt.Fetch(ctx)
		log.Debugw("finished fetching from mount/upgrader for shard acquire", "shard", s.key, "error", err)
		return err
	})

	if err != nil {
		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// fail the shard
		_ = d.failShard(s, d.completionCh, "failed to acquire reader of mount so we can return the accessor: %w", err)

		// send the shard error to the caller.
		d.dispatchResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	idx, err := d.indices.GetFullIndex(k)
	if err != nil {
		log.Debugw("failed to get index for shard while acquiring shard", "shard", s.key)
		if err := reader.Close(); err != nil {
			log.Errorf("failed to close mount reader: %s", err)
		}

		// release the shard to decrement the refcount that's incremented before `acquireAsync` is called.
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// fail the shard
		_ = d.failShard(s, d.completionCh, "failed to recover index for shard %s: %w", k, err)

		// send the shard error to the caller.
		d.dispatchResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	log.Debugw("successfully fetched underlying mount data and index for acquiring shard", "shard", s.key)
	sa, err := NewShardAccessor(reader, idx, s)

	// send the shard accessor to the caller.
	d.dispatchResult(&ShardResult{Key: k, Accessor: sa, Error: err}, w)
}

// initializeShard initializes a shard asynchronously by fetching its data and
// performing indexing.
func (d *DAGStore) initializeShard(ctx context.Context, s *Shard, mnt mount.Mount) {
	var reader mount.Reader
	err := d.throttleFetch.Do(ctx, func(ctx context.Context) error {
		var err error
		reader, err = mnt.Fetch(ctx)
		log.Debugw("finished fetching from mount/upgrader for initializing shard", "shard", s.key, "error", err)
		return err
	})

	if err != nil {
		_ = d.failShard(s, d.completionCh, "failed to acquire reader of mount so that we can initialize the shard: %w", err)
		return
	}
	defer reader.Close()

	// works for both CARv1 and CARv2.
	var idx index.Index
	err = d.throttleIndex.Do(ctx, func(_ context.Context) error {
		var err error
		idx, err = car.ReadOrGenerateIndex(reader, car.ZeroLengthSectionAsEOF(true))
		log.Debugw("finished generating index for shard", "shard", s.key, "error", err)
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

	_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
}

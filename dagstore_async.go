package dagstore

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/ipld/go-car/v2"
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
	if err != nil {
		_ = d.failShard(s, fmt.Errorf("failed to acquire reader of mount: %w", err), d.completionCh)
		d.sendResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	idx, err := d.indices.GetFullIndex(k)
	if err != nil {
		if err := reader.Close(); err != nil {
			log.Errorf("failed to close mount reader: %s", err)
		}
		_ = d.failShard(s, fmt.Errorf("failed to recover index for shard %s: %w", k, err), d.completionCh)
		d.sendResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	sa, err := NewShardAccessor(k, reader, idx, s)
	d.sendResult(&ShardResult{Key: k, Accessor: sa, Error: err}, w)
}

// initializeAsync initializes a shard asynchronously by fetching its data and
// performing indexing.
func (d *DAGStore) initializeAsync(ctx context.Context, s *Shard, mnt mount.Mount) {
	reader, err := mnt.Fetch(ctx)
	if err != nil {
		_ = d.failShard(s, fmt.Errorf("failed to acquire reader of mount: %w", err), d.completionCh)
		return
	}
	defer reader.Close()
	// works for both CARv1 and CARv2.
	idx, err := car.ReadOrGenerateIndex(reader)
	if err != nil {
		_ = d.failShard(s, fmt.Errorf("failed to read/generate CAR Index: %w", err), d.completionCh)
		return
	}
	if err := d.indices.AddFullIndex(s.key, idx); err != nil {
		_ = d.failShard(s, fmt.Errorf("failed to add index for shard: %w", err), d.completionCh)
		return
	}

	_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
}

func (d *DAGStore) failShard(s *Shard, err error, ch chan *task) error {
	return d.queueTask(&task{op: OpShardFail, shard: s, err: err}, ch)
}

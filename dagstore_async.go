package dagstore

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/ipld/go-car/v2"
	carindex "github.com/ipld/go-car/v2/index"
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
		err = fmt.Errorf("failed to acquire reader of mount: %w", err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		w.dispatch(ShardResult{Key: k, Error: err})
		return
	}

	idx, err := d.indices.GetFullIndex(k)
	if err != nil {
		err = fmt.Errorf("failed to recover index for shard %s: %w", k, err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		w.dispatch(ShardResult{Key: k, Error: err})
		return
	}

	sa, err := NewShardAccessor(k, reader, idx)
	w.dispatch(ShardResult{Key: k, Accessor: sa, Error: err})
}

// initializeAsync initializes a shard asynchronously by fetching its data and
// performing indexing.
func (d *DAGStore) initializeAsync(ctx context.Context, s *Shard, mnt mount.Mount) {
	reader, err := mnt.Fetch(ctx)
	if err != nil {
		err = fmt.Errorf("failed to acquire reader of mount: %w", err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		return
	}

	defer reader.Close()

	carreader, err := car.NewReader(reader)
	if err != nil {
		err = fmt.Errorf("failed to read car: %w", err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		return
	}

	if has := carreader.Header.HasIndex(); !has {
		err = fmt.Errorf("processing of unindexed cars unimplemented")
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		return
	}

	ir := carreader.IndexReader()
	idx, err := carindex.ReadFrom(ir)
	if err != nil {
		err = fmt.Errorf("failed to read carv2 index: %w", err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		return
	}

	err = d.indices.AddFullIndex(s.key, idx)
	if err != nil {
		err = fmt.Errorf("failed to add index for shard: %w", err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.internalCh)
		return
	}

	_ = d.queueTask(&Task{Op: OpShardMakeAvailable, Shard: s, Index: idx}, d.completionCh)
}

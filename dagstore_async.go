package dagstore

import (
	"context"
	"errors"
	"fmt"
	"io"

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
		_ = d.queueTask(&task{op: OpShardFail, shard: s, err: err}, d.completionCh)
		d.sendResult(&ShardResult{Key: k, Error: err}, w)
		return
	}

	idx, err := d.indices.GetFullIndex(k)
	if err != nil {
		err = fmt.Errorf("failed to recover index for shard %s: %w", k, err)
		_ = d.queueTask(&task{op: OpShardFail, shard: s, err: err}, d.completionCh)
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
		d.failShard(s, fmt.Errorf("failed to acquire reader of mount: %w", err))
		return
	}
	defer reader.Close()

	// if we already have the index for this shard, there's nothing to do here.
	if istat, err := d.indices.StatFullIndex(s.key); err == nil && istat.Exists {
		_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
		return
	}

	// otherwise, generate and persist an Index for the CAR payload of the given Shard.
	// TODO Replace the below with https://github.com/ipld/go-car/issues/143 when complete
	// determine CAR version and Index accordingly.
	ver, err := car.ReadVersion(reader)
	if err != nil {
		d.failShard(s, fmt.Errorf("failed to read CAR version: %w", err))
		return
	}
	// seek to start of file as reading the version above will change the offset.
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		d.failShard(s, fmt.Errorf("failed to seek to CAR start: %w", err))
		return
	}

	var idx carindex.Index
	switch ver {
	case 2:
		carreader, err := car.NewReader(reader)
		if err != nil {
			d.failShard(s, fmt.Errorf("failed to create carv2 reader: %w", err))
			return
		}
		if has := carreader.Header.HasIndex(); !has {
			d.failShard(s, errors.New(("processing of unindexed carV2s unimplemented")))
			return
		}
		idx, err = carindex.ReadFrom(carreader.IndexReader())
		if err != nil {
			d.failShard(s, fmt.Errorf("failed to read shard index: %w", err))
			return
		}
	case 1:
		cidx, err := carindex.Generate(reader)
		if err != nil {
			d.failShard(s, fmt.Errorf("failed to generate Index for CARv1: %w", err))
			return
		}
		idx = cidx
	default:
		d.failShard(s, fmt.Errorf("invalid CAR version %d", err))
		return
	}

	if err := d.indices.AddFullIndex(s.key, idx); err != nil {
		d.failShard(s, fmt.Errorf("failed to add index for shard: %w", err))
		return
	}

	_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
}

func (d *DAGStore) failShard(s *Shard, err error) {
	_ = d.queueTask(&task{op: OpShardFail, shard: s, err: err}, d.internalCh)
}

package dagstore

import (
	"fmt"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/ipld/go-car/v2"
	carindex "github.com/ipld/go-car/v2/index"
)

// This file contains methods that are called from the event loop
// but are run asynchronously in dedicated goroutines.

// acquireAsync acquires a shard by fetching its data, obtaining its index, and
// joining them to form a ShardAccessor.
func (d *DAGStore) acquireAsync(acqCh chan ShardResult, s *Shard, mnt mount.Mount) {
	k := s.key
	reader, err := mnt.Fetch(d.ctx)
	if err != nil {
		err = fmt.Errorf("failed to acquire reader of mount: %w", err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		acqCh <- ShardResult{Key: k, Error: err}
		return
	}

	idx, err := d.indices.GetFullIndex(k)
	if err != nil {
		err = fmt.Errorf("failed to recover index for shard %s: %w", k, err)
		_ = d.queueTask(&Task{Op: OpShardFail, Shard: s, Error: err}, d.completionCh)
		acqCh <- ShardResult{Key: k, Error: err}
		return
	}

	sa, err := NewShardAccessor(k, reader, idx)
	acqCh <- ShardResult{Key: k, Accessor: sa, Error: err}
}

// loadIndexAsync loads the index from a shard's CAR file. Currently it supports
// only indexed CARv2
func loadIndexAsync(reader mount.Reader) (carindex.Index, error) {
	carreader, err := car.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read car: %w", err)
	}

	if has := carreader.Header.HasIndex(); has {
		ir := carreader.IndexReader()
		idx, err := carindex.ReadFrom(ir)
		if err != nil {
			return nil, fmt.Errorf("failed to read carv index: %w", err)
		}
		return idx, nil
	}
	return nil, fmt.Errorf("processing of unindexed cars unimplemented")
}

package dagstore

import (
	"context"
	"fmt"
	"io"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-data-segment/datasegment"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipld/go-car/v2"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

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
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// fail the shard
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
		_ = d.queueTask(&task{op: OpShardRelease, shard: s}, d.completionCh)

		// fail the shard
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
	stat, err := mnt.Stat(ctx)
	if err != nil {
		log.Warnw("initialize: failed to stat from mount upgrader", "shard", s.key, "error", err)

		_ = d.failShard(s, d.completionCh, "failed to get size of mount on initialization: %w", err)
		return
	}
	reader, err := mnt.Fetch(ctx)
	if err != nil {
		log.Warnw("initialize: failed to fetch from mount upgrader", "shard", s.key, "error", err)

		_ = d.failShard(s, d.completionCh, "failed to acquire reader of mount on initialization: %w", err)
		return
	}
	defer reader.Close()

	log.Debugw("initialize: successfully fetched from mount upgrader", "shard", s.key)

	// attempt to get items by checking for a data segment index
	dsIdx, err := d.parseShardWithDataSegmentIndex(ctx, s.key, stat.Size, reader)
	if err == nil {
		if err := d.indices.AddFullIndex(s.key, dsIdx); err != nil {
			_ = d.failShard(s, d.completionCh, "failed to add index for shard: %w", err)
			return
		}
		mhIter := &mhIdx{iterableIdx: dsIdx}
		if err := d.TopLevelIndex.AddMultihashesForShard(ctx, mhIter, s.key); err != nil {
			log.Errorw("failed to add shard multihashes to the inverted index", "shard", s.key, "error", err)
		}
		_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
		return
	}
	log.Debugw("initialize: falling back to standard car parse.", "err", err, "shard", s.key)
	if _, err := reader.Seek(0, 0); err != nil {
		log.Warnw("initialize: failed to rewind mount", "shard", s.key, "error", err)

		_ = d.failShard(s, d.completionCh, "failed to rewind reader of mount on initialization: %w", err)
		return
	}

	// works for both CARv1 and CARv2.
	var idx carindex.Index
	err = d.throttleIndex.Do(ctx, func(_ context.Context) error {
		var err error
		idx, err = car.ReadOrGenerateIndex(reader, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
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

	// add all cids in the shard to the inverted (cid -> []Shard Keys) index.
	iterableIdx, ok := idx.(carindex.IterableIndex)
	if ok {
		mhIter := &mhIdx{iterableIdx: iterableIdx}
		if err := d.TopLevelIndex.AddMultihashesForShard(ctx, mhIter, s.key); err != nil {
			log.Errorw("failed to add shard multihashes to the inverted index", "shard", s.key, "error", err)
		}
	} else {
		log.Errorw("shard index is not iterable", "shard", s.key)
	}

	_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.completionCh)
}

func (d *DAGStore) parseShardWithDataSegmentIndex(ctx context.Context, sKey shard.Key, size int64, r mount.Reader) (carindex.IterableIndex, error) {
	ps := abi.UnpaddedPieceSize(size).Padded()
	dsis := datasegment.DataSegmentIndexStartOffset(ps)
	if _, err := r.Seek(int64(dsis), io.SeekStart); err != nil {
		return nil, fmt.Errorf("could not seek to data segment index: %w", err)
	}
	dataSegments, err := datasegment.ParseDataSegmentIndex(r)
	if err != nil {
		return nil, fmt.Errorf("could not parse data segment index: %w", err)
	}
	segments, err := dataSegments.ValidEntries()
	if err != nil {
		return nil, fmt.Errorf("could not calculate valid entries: %w", err)
	}

	finalIdx := carindex.NewInsertionIndex()
	for _, s := range segments {
		segOffset := s.UnpaddedOffest()
		segSize := s.UnpaddedLength()
		var idx carindex.Index
		err = d.throttleIndex.Do(ctx, func(_ context.Context) error {
			var err error

			lr := io.NewSectionReader(r, int64(segOffset), int64(segSize))
			idx, err = car.ReadOrGenerateIndex(lr, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
			if err == nil {
				log.Debugw("initialize: finished generating index for shard", "shard", sKey)
			} else {
				log.Warnw("initialize: failed to generate index for shard", "shard", sKey, "error", err)
			}
			return err
		})
		if err == nil {
			if mhi, ok := idx.(*carindex.MultihashIndexSorted); ok {
				_ = mhi.ForEach(func(mh multihash.Multihash, offset uint64) error {
					finalIdx.InsertNoReplace(cid.NewCidV1(uint64(multicodec.Raw), mh), segOffset+offset)
					return nil
				})
			} else {
				log.Debugw("initialize: Unexpected index format on generation in shard", "shard", sKey, "offset", segOffset)
			}
		}
	}

	return finalIdx, nil
}

// Convenience struct for converting from CAR index.IterableIndex to the
// iterator required by the dag store inverted index.
type mhIdx struct {
	iterableIdx carindex.IterableIndex
}

var _ index.MultihashIterator = (*mhIdx)(nil)

func (it *mhIdx) ForEach(fn func(mh multihash.Multihash) error) error {
	return it.iterableIdx.ForEach(func(mh multihash.Multihash, _ uint64) error {
		return fn(mh)
	})
}

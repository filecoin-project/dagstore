package index

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ipfs/go-datastore/namespace"

	ds "github.com/ipfs/go-datastore"

	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/shard"
)

var _ Inverted = (*invertedIndexImpl)(nil)

type invertedIndexImpl struct {
	mu sync.Mutex
	ds ds.Batching
}

// NewInverted returns a new inverted index that uses `go-indexer-core`
// as it's storage backend. We use `go-indexer-core` as the backend here
// as it's been optimized to store (multihash -> Value) kind of data and
// supports bulk updates via context ID and metadata-deduplication which are useful properties for our use case here.
func NewInverted(dts ds.Batching) *invertedIndexImpl {
	dts = namespace.Wrap(dts, ds.NewKey("/inverted/index"))
	return &invertedIndexImpl{
		ds: dts,
	}
}

func (d *invertedIndexImpl) AddMultihashesForShard(ctx context.Context, mhIter MultihashIterator, s shard.Key) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	batch, err := d.ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create ds batch: %w", err)
	}

	if err := mhIter.ForEach(func(mh multihash.Multihash) error {
		key := ds.NewKey(string(mh))
		// do we already have an entry for this multihash ?
		val, err := d.ds.Get(ctx, key)
		if err != nil && err != ds.ErrNotFound {
			return fmt.Errorf("failed to get value for multihash %s, err: %w", mh, err)
		}

		// if we don't have an existing entry for this mh, create one
		if err == ds.ErrNotFound {
			s := []shard.Key{s}
			bz, err := json.Marshal(s)
			if err != nil {
				return fmt.Errorf("failed to marshal shard list to bytes: %w", err)
			}
			if err := batch.Put(ctx, key, bz); err != nil {
				return fmt.Errorf("failed to put mh=%s, err=%w", mh, err)
			}
			return nil
		}

		// else , append the shard key to the existing list
		var es []shard.Key
		if err := json.Unmarshal(val, &es); err != nil {
			return fmt.Errorf("failed to unmarshal shard keys: %w", err)
		}

		// if we already have the shard key indexed for the multihash, nothing to do here.
		if has(es, s) {
			return nil
		}

		es = append(es, s)
		bz, err := json.Marshal(es)
		if err != nil {
			return fmt.Errorf("failed to marshal shard keys: %w", err)
		}
		if err := batch.Put(ctx, key, bz); err != nil {
			return fmt.Errorf("failed to put mh=%s, err%w", mh, err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to add index entry: %w", err)
	}

	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	if err := d.ds.Sync(ctx, ds.Key{}); err != nil {
		return fmt.Errorf("failed to sync puts: %w", err)
	}

	return nil
}

func (d *invertedIndexImpl) GetShardsForMultihash(ctx context.Context, mh multihash.Multihash) ([]shard.Key, error) {
	key := ds.NewKey(string(mh))
	sbz, err := d.ds.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index for mh %s, err: %w", mh, err)
	}

	var shardKeys []shard.Key
	if err := json.Unmarshal(sbz, &shardKeys); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shard keys for mh=%s, err=%w", mh, err)
	}

	return shardKeys, nil
}

func has(es []shard.Key, k shard.Key) bool {
	for _, s := range es {
		if s == k {
			return true
		}
	}
	return false
}

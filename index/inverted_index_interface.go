package index

import (
	"context"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/multiformats/go-multihash"
)

type MultihashIterator interface {
	ForEach(func(mh multihash.Multihash) error) error
}

// Inverted is the top-level inverted index that maps a multihash to all the shards it is present in.
type Inverted interface {
	// AddMultihashesForShard adds a (multihash -> shard key) mapping for all multihashes returned by the given MultihashIterator.
	AddMultihashesForShard(ctx context.Context, mhIter MultihashIterator, s shard.Key) error
	// GetShardsForMultihash returns keys for all the shards that has the given multihash.
	GetShardsForMultihash(ctx context.Context, h multihash.Multihash) ([]shard.Key, error)
}

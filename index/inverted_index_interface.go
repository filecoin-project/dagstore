package index

import (
	"github.com/filecoin-project/dagstore/shard"
	"github.com/multiformats/go-multihash"
)

type MultihashIterator interface {
	ForEach(func(mh multihash.Multihash) error) error
}

// Inverted is the top-level inverted index that maps a multihash to all the shards it is present in.
type Inverted interface {
	// AddMultihashesForShard adds a (multihash -> shard key) mapping for all multihashes returned by the given MultihashIterator.
	AddMultihashesForShard(mhIter MultihashIterator, s shard.Key) error

	// DeleteMultihashesForShard deletes the (cid -> shard key) mapping for all multihashes returned by the given MultihashIterator.
	DeleteMultihashesForShard(s shard.Key, mhIter MultihashIterator) error

	// GetShardsForMultihash returns keys for all the shards that has the given multihash.
	GetShardsForMultihash(h multihash.Multihash) ([]shard.Key, error)

	// Size returns the total bytes of storage used to store the indexed
	// content in persistent storage.
	Size() (int64, error)
}

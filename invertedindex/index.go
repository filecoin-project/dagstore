package invertedindex

import (
	"github.com/filecoin-project/dagstore/shard"
	"github.com/multiformats/go-multihash"
)

type MultihashIterator interface {
	ForEach(func(mh multihash.Multihash) error) error
}

type Index interface {
	AddMultihashesForShard(mhIter MultihashIterator, s shard.Key) error

	DeleteMultihashesForShard(s shard.Key, mhIter MultihashIterator) error

	GetShardsForMultihash(h multihash.Multihash) ([]shard.Key, error)

	Size() (int64, error)
}

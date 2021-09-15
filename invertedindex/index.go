package invertedindex

import (
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
)

type MultihashIterator interface {
	ForEach(func(mh multihash.Multihash) error) error
}

type Index interface {
	AddMultihashesForShard(mhIter MultihashIterator, s shard.Key) error

	DeleteMultihashesForShard(s shard.Key, mhIter MultihashIterator) error

	GetShardsForCid(c cid.Cid) ([]shard.Key, error)

	// size on disk
	Size() (int64, error)

	NCids() (int64, error)

	//Iterator() (Iterator, error)
}

type IndexEntry struct {
	Multihash mh.Multihash
	Shards    []shard.Key
}

type Iterator interface {
	Next() (has bool, entry IndexEntry, err error)
}

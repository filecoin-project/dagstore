package invertedindex

import (
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

type Index interface {
	AddMultihashesForShard(mhIter index.IterableIndex, s shard.Key) error

	DeleteMultihashesForShard(s shard.Key, mhIter index.IterableIndex) error

	GetShardsForCid(c cid.Cid) ([]shard.Key, error)

	NMultihashes() (uint64, error)

	Iterator() (Iterator, error)
}

type IndexEntry struct {
	Multihash mh.Multihash
	Shards    []shard.Key
}

type Iterator interface {
	Next() (has bool, entry IndexEntry, err error)
}

package invertedindex

import (
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
)

// TODO Replace with CAR Index iterator once Data-systems finishes it.
type CidIterator []cid.Cid

type Index interface {
	AddCidsForShard(cidIter CidIterator, s shard.Key) error

	DeleteCidsForShard(s shard.Key, cidIterator CidIterator) error

	GetShardsForCid(c cid.Cid) ([]shard.Key, error)

	NCids() (uint64, error)

	Iterator() (Iterator, error)
}

type IndexEntry struct {
	Cid    cid.Cid
	Shards []shard.Key
}

type Iterator interface {
	Next() (has bool, entry IndexEntry, err error)
}

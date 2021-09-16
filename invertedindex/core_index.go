package invertedindex

import (
	"errors"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"

	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/shard"
)

var ErrNotFound = errors.New("multihash not found in Index")

var _ Index = (*IndexerCoreIndex)(nil)

type IndexerCoreIndex struct {
	is indexer.Interface
}

func NewIndexerCore(is indexer.Interface) *IndexerCoreIndex {
	return &IndexerCoreIndex{
		is: is,
	}
}

func (d *IndexerCoreIndex) AddMultihashesForShard(mhIter MultihashIterator, s shard.Key) error {
	return mhIter.ForEach(func(mh multihash.Multihash) error {
		// go-indexer-core appends values to the existing values we already have for the key
		// it also takes care of de-duplicating values.
		_, err := d.is.Put(mh, valueForShardKey(s))
		return err
	})
}

func (d *IndexerCoreIndex) DeleteMultihashesForShard(sk shard.Key, mhIter MultihashIterator) error {
	return mhIter.ForEach(func(mh multihash.Multihash) error {
		// remove the given value i.e. shard key from the index for the given multihash.
		_, err := d.is.Remove(mh, valueForShardKey(sk))
		return err
	})
}

func (d *IndexerCoreIndex) GetShardsForMultihash(mh multihash.Multihash) ([]shard.Key, error) {
	values, found, err := d.is.Get(mh)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index for multihash %s, err: %w", mh, err)
	}
	if !found || len(values) == 0 {
		return nil, fmt.Errorf("cid not found, multihash=%s, err: %w", mh, ErrNotFound)
	}

	shardKeys := make([]shard.Key, 0, len(values))
	for _, v := range values {
		shardKeys = append(shardKeys, shardKeyFromValue(v))
	}

	return shardKeys, nil
}

func (d *IndexerCoreIndex) Size() (int64, error) {
	return d.is.Size()
}

func shardKeyFromValue(val indexer.Value) shard.Key {
	str := string(val.Metadata)
	return shard.KeyFromString(str)
}

func valueForShardKey(key shard.Key) indexer.Value {
	return indexer.Value{
		Metadata: []byte(key.String()),
	}
}

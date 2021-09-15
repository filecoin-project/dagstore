package invertedindex

import (
	"errors"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/shard"
)

var ErrNotFound = errors.New("cid not found in Index")

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
		// remove the given value i.e. shard key from the index for the given cid key.
		_, err := d.is.Remove(mh, valueForShardKey(sk))
		return err
	})
}

func (d *IndexerCoreIndex) GetShardsForCid(c cid.Cid) ([]shard.Key, error) {
	mh := c.Hash()
	values, found, err := d.is.Get(mh)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index for cid %s, err: %w", c, err)
	}
	if !found || len(values) == 0 {
		return nil, fmt.Errorf("cid not found, cid=%s, err: %w", c, ErrNotFound)
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

// TODO: Index till dosent's support this
func (d *IndexerCoreIndex) NCids() (int64, error) {
	return 0, nil
}

/*func (d *IndexerCoreIndex) Iterator() (Iterator, error) {
	results, err := d.ds.Query(query.Query{})
	if err != nil {
		return nil, fmt.Errorf("failed to recover dagstore state from store: %w", err)
	}
	return &iteratorImpl{results}, nil
}

var _ Iterator = (*iteratorImpl)(nil)

type iteratorImpl struct {
	res query.Results
}

func (i *iteratorImpl) Next() (has bool, entry IndexEntry, err error) {
	res, has := i.res.NextSync()
	if !has {
		return has, IndexEntry{}, nil
	}

	k := res.Key
	if k[0] == '/' {
		k = k[1:]
	}
	mh, err := multihash.FromHexString(k)
	if err != nil {
		return false, IndexEntry{}, fmt.Errorf("failed to decode cid=%s, err=%w", res.Key, err)
	}

	var shardKeys []shard.Key

	if err := json.Unmarshal(res.Value, &shardKeys); err != nil {
		return false, IndexEntry{}, fmt.Errorf("failed to unmarshal shard keys, err=%w", err)
	}

	return true, IndexEntry{
		Multihash: mh,
		Shards:    shardKeys,
	}, nil
}*/

func shardKeyFromValue(val indexer.Value) shard.Key {
	str := string(val.Metadata)
	return shard.KeyFromString(str)
}

func valueForShardKey(key shard.Key) indexer.Value {
	return indexer.Value{
		Metadata: []byte(key.String()),
	}
}

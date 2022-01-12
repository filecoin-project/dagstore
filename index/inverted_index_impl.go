package index

import (
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/filecoin-project/go-indexer-core"

	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/shard"
)

var InvertedIndexErrNotFound = errors.New("multihash not found in Index")

var _ Inverted = (*indexerCoreIndex)(nil)

type indexerCoreIndex struct {
	is         indexer.Interface
	selfPeerID peer.ID
}

// NewInverted returns a new inverted index that uses `go-indexer-core`
// as it's storage backend. We use `go-indexer-core` as the backend here
// as it's been optimized to store (multihash -> Value) kind of data and
// supports bulk updates via context ID and metadata-deduplication which are useful properties for our use case here.
func NewInverted(is indexer.Interface, selfPeerID peer.ID) *indexerCoreIndex {
	return &indexerCoreIndex{
		is:         is,
		selfPeerID: selfPeerID,
	}
}

func (d *indexerCoreIndex) AddMultihashesForShard(mhIter MultihashIterator, s shard.Key) error {
	return mhIter.ForEach(func(mh multihash.Multihash) error {
		// go-indexer-core appends values to the existing values we already have for the key
		// it also takes care of de-duplicating values.
		return d.is.Put(valueForShardKey(s, d.selfPeerID), mh)
	})
}

func (d *indexerCoreIndex) DeleteMultihashesForShard(sk shard.Key, mhIter MultihashIterator) error {
	return mhIter.ForEach(func(mh multihash.Multihash) error {
		// remove the given value i.e. shard key from the index for the given multihash.
		return d.is.Remove(valueForShardKey(sk, d.selfPeerID), mh)
	})
}

func (d *indexerCoreIndex) GetShardsForMultihash(mh multihash.Multihash) ([]shard.Key, error) {
	values, found, err := d.is.Get(mh)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index for multihash %s, err: %w", mh, err)
	}
	if !found || len(values) == 0 {
		return nil, fmt.Errorf("cid not found, multihash=%s, err: %w", mh, InvertedIndexErrNotFound)
	}

	shardKeys := make([]shard.Key, 0, len(values))
	for _, v := range values {
		shardKeys = append(shardKeys, shardKeyFromValue(v))
	}

	return shardKeys, nil
}

func (d *indexerCoreIndex) Size() (int64, error) {
	return d.is.Size()
}

func shardKeyFromValue(val indexer.Value) shard.Key {
	str := string(val.ContextID)
	return shard.KeyFromString(str)
}

func valueForShardKey(key shard.Key, selfPeerID peer.ID) indexer.Value {
	return indexer.Value{
		ProviderID:    selfPeerID,
		ContextID:     []byte(key.String()),
		MetadataBytes: []byte("N/A"),
	}
}

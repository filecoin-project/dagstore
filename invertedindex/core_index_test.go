package invertedindex

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core/store/memory"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/dagstore/shard"
)

func TestDatastoreIndexEmpty(t *testing.T) {
	req := require.New(t)

	cid1, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q7")
	req.NoError(err)

	idx := NewIndexerCore(memory.New())

	l, err := idx.Size()
	req.NoError(err)
	req.EqualValues(0, l)

	_, err = idx.GetShardsForMultihash(cid1.Hash())
	req.True(xerrors.Is(err, ErrNotFound))
}

func TestDatastoreIndex(t *testing.T) {
	req := require.New(t)

	cid1, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q7")
	req.NoError(err)
	cid2, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q8")
	req.NoError(err)
	cid3, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q9")
	req.NoError(err)

	h1 := cid1.Hash()
	h2 := cid2.Hash()
	h3 := cid3.Hash()

	idx := NewIndexerCore(memory.New())

	// Add hash to shard key mappings for h1, h2:
	// h1 -> [shard-key-1]
	// h2 -> [shard-key-1]
	itIdxA := &mhIt{[]multihash.Multihash{h1, h2}}
	sk1 := shard.KeyFromString("shard-key-1")
	err = idx.AddMultihashesForShard(itIdxA, sk1)
	req.NoError(err)

	// Add hash to shard key mappings for h1, h3:
	// h1 -> [shard-key-1, shard-key-2]
	// h3 -> [shard-key-2]
	itIdxB := &mhIt{[]multihash.Multihash{h1, h3}}
	sk2 := shard.KeyFromString("shard-key-2")
	err = idx.AddMultihashesForShard(itIdxB, sk2)
	req.NoError(err)

	// Verify h1 mapping:
	// h1 -> [shard-key-1, shard-key-2]
	shards, err := idx.GetShardsForMultihash(cid1.Hash())
	req.NoError(err)
	req.Len(shards, 2)
	req.Contains(shards, sk1)
	req.Contains(shards, sk2)

	// Verify h2 mapping:
	// h2 -> [shard-key-1]
	shards, err = idx.GetShardsForMultihash(cid2.Hash())
	req.NoError(err)
	req.Len(shards, 1)
	req.Equal(shards[0], sk1)
}

func TestDatastoreIndexDelete(t *testing.T) {
	req := require.New(t)

	cid1, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q7")
	req.NoError(err)
	cid2, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q8")
	req.NoError(err)

	h1 := cid1.Hash()
	h2 := cid2.Hash()

	idx := NewIndexerCore(memory.New())

	// Add hash to shard key mappings for h1, h2:
	// h1 -> [shard-key-1]
	// h2 -> [shard-key-1]
	itIdxA := &mhIt{[]multihash.Multihash{h1, h2}}
	sk1 := shard.KeyFromString("shard-key-1")
	err = idx.AddMultihashesForShard(itIdxA, sk1)
	req.NoError(err)

	// Remove mapping from h1 -> [shard-key-1]
	err = idx.DeleteMultihashesForShard(sk1, &mhIt{[]multihash.Multihash{h1}})
	req.NoError(err)

	// Verify that the hash is h2 (not h1)
	shards, err := idx.GetShardsForMultihash(cid2.Hash())
	req.NoError(err)
	req.Len(shards, 1)
	req.Contains(shards, sk1)
}

type mhIt struct {
	mhs []multihash.Multihash
}

var _ MultihashIterator = (*mhIt)(nil)

func (mi *mhIt) ForEach(f func(mh multihash.Multihash) error) error {
	for _, mh := range mi.mhs {
		if err := f(mh); err != nil {
			return err
		}
	}
	return nil
}

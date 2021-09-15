package invertedindex

import (
	"testing"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/dagstore/shard"
)

func TestDatastoreIndexEmpty(t *testing.T) {
	req := require.New(t)

	cid1, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q7")
	req.NoError(err)

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	idx := NewDataStoreIndex(dstore)

	l, err := idx.Length()
	req.NoError(err)
	req.EqualValues(0, l)

	_, err = idx.GetShardsForCid(cid1)
	req.True(xerrors.Is(err, ds.ErrNotFound))

	it, err := idx.Iterator()
	req.NoError(err)
	has, _, err := it.Next()
	req.NoError(err)
	req.False(has)
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

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	idx := NewDataStoreIndex(dstore)

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

	// Verify there are three hashes in index
	l, err := idx.Length()
	req.NoError(err)
	req.EqualValues(3, l)

	// Verify h1 mapping:
	// h1 -> [shard-key-1, shard-key-2]
	shards, err := idx.GetShardsForCid(cid1)
	req.NoError(err)
	req.Len(shards, 2)
	req.Contains(shards, sk1)
	req.Contains(shards, sk2)

	// Verify h2 mapping:
	// h2 -> [shard-key-1]
	shards, err = idx.GetShardsForCid(cid2)
	req.NoError(err)
	req.Len(shards, 1)
	req.Equal(shards[0], sk1)

	// Verify iterator
	it, err := idx.Iterator()
	req.NoError(err)

	checkEntry := func(e IndexEntry) {
		switch e.Multihash.String() {

		// h1 -> [shard-key-1, shard-key-2]
		case h1.String():
			req.Len(e.Shards, 2)
			req.Contains(e.Shards, sk1)
			req.Contains(e.Shards, sk2)

		// h2 -> [shard-key-1]
		case h2.String():
			req.Len(e.Shards, 1)
			req.Contains(e.Shards, sk1)

		// h3 -> [shard-key-2]
		case h3.String():
			req.Len(e.Shards, 1)
			req.Contains(e.Shards, sk2)
		}
	}

	// Iterator should return three results
	for i := 0; i < 3; i++ {
		has, itentry, err := it.Next()
		req.NoError(err)
		req.True(has)
		checkEntry(itentry)
	}

	// Should return has == false after three results
	has, _, err := it.Next()
	req.NoError(err)
	req.False(has)
}

func TestDatastoreIndexDelete(t *testing.T) {
	req := require.New(t)

	cid1, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q7")
	req.NoError(err)
	cid2, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q8")
	req.NoError(err)

	h1 := cid1.Hash()
	h2 := cid2.Hash()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	idx := NewDataStoreIndex(dstore)

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

	// Verify there is now only one hash in index
	l, err := idx.Length()
	req.NoError(err)
	req.EqualValues(1, l)

	// Verify that the hash is h2 (not h1)
	shards, err := idx.GetShardsForCid(cid2)
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

package index

import (
	"context"
	"testing"

	blocksutil "github.com/ipfs/go-ipfs-blocksutil"

	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/ipfs/go-datastore/sync"

	ds "github.com/ipfs/go-datastore"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/dagstore/shard"
)

var blockGenerator = blocksutil.NewBlockGenerator()

func TestDatastoreIndexEmpty(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)

	cid1, err := cid.Parse("Qmard76Snyj9VCJBzLSLYzXnJJ2BnyCN2KAfAkpLXyt1q7")
	req.NoError(err)

	idx := NewInverted(sync.MutexWrap(ds.NewMapDatastore()))

	_, err = idx.GetShardsForMultihash(ctx, cid1.Hash())
	req.True(xerrors.Is(err, ds.ErrNotFound))
}

func TestLevelDBBatch(t *testing.T) {
	ctx := context.Background()
	// Create a new LevelDB datastore
	dstore, err := levelds.NewDatastore(t.TempDir(), &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
	require.NoError(t, err)
	idx := NewInverted(dstore)

	// add 50,000 multihashes
	mhs := GenerateMhs(100000)
	require.Len(t, mhs, 100000)
	itIdxA := &mhIt{mhs}
	sk1 := shard.KeyFromString("shard-key-1")
	err = idx.AddMultihashesForShard(ctx, itIdxA, sk1)
	require.NoError(t, err)

	for _, mh := range mhs {
		sk, err := idx.GetShardsForMultihash(ctx, mh)
		require.NoError(t, err)
		require.Len(t, sk, 1)
		require.Contains(t, sk, sk1)
	}
}

func TestDatastoreIndex(t *testing.T) {
	ctx := context.Background()
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

	idx := NewInverted(sync.MutexWrap(ds.NewMapDatastore()))

	// Add hash to shard key mappings for h1, h2: also dedupes the multihash iterator
	// h1 -> [shard-key-1]
	// h2 -> [shard-key-1]
	itIdxA := &mhIt{[]multihash.Multihash{h1, h2, h1, h1}}
	sk1 := shard.KeyFromString("shard-key-1")
	err = idx.AddMultihashesForShard(ctx, itIdxA, sk1)
	req.NoError(err)

	// Add hash to shard key mappings for h1, h3:
	// h1 -> [shard-key-1, shard-key-2]
	// h3 -> [shard-key-2]
	itIdxB := &mhIt{[]multihash.Multihash{h1, h3}}
	sk2 := shard.KeyFromString("shard-key-2")
	err = idx.AddMultihashesForShard(ctx, itIdxB, sk2)
	req.NoError(err)

	// add shard-key-1 again for h1 -> will get de-duped
	itIdxC := &mhIt{[]multihash.Multihash{h1}}
	err = idx.AddMultihashesForShard(ctx, itIdxC, sk1)
	req.NoError(err)

	// Verify h1 mapping:
	// h1 -> [shard-key-1, shard-key-2]
	shards, err := idx.GetShardsForMultihash(ctx, cid1.Hash())
	req.NoError(err)
	req.Len(shards, 2)
	req.Contains(shards, sk1)
	req.Contains(shards, sk2)

	// Verify h2 mapping:
	// h2 -> [shard-key-1]
	shards, err = idx.GetShardsForMultihash(ctx, cid2.Hash())
	req.NoError(err)
	req.Len(shards, 1)
	req.Equal(shards[0], sk1)
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

// GenerateCids produces n content identifiers.
func GenerateMhs(n int) []multihash.Multihash {
	mhs := make([]multihash.Multihash, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		mhs = append(mhs, c.Hash())
	}
	return mhs
}

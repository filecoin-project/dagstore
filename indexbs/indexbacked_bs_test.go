package indexbs

import (
	"context"
	"errors"
	"testing"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/testdata"

	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

var noOpSelector = func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
	return shards[0], nil
}

var carv2mnt = &mount.FSMount{FS: testdata.FS, Path: testdata.FSPathCarV2}

func TestReadOnlyBs(t *testing.T) {
	ctx := context.Background()
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	dagst, err := dagstore.NewDAGStore(dagstore.Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     store,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register a shard
	ch := make(chan dagstore.ShardResult, 1)
	sk := shard.KeyFromString("test1")
	err = dagst.RegisterShard(context.Background(), sk, carv2mnt, ch, dagstore.RegisterOpts{})
	require.NoError(t, err)
	res := <-ch
	require.NoError(t, res.Error)

	rbs, err := NewIndexBackedBlockstore(dagst, noOpSelector, 10, 10)
	require.NoError(t, err)

	// iterate over the CARV2 Index for the given CARv2 file and ensure the readonly blockstore
	// works for each of those cids
	it, err := dagst.GetIterableIndex(sk)
	require.NoError(t, err)

	it.ForEach(func(mh multihash.Multihash, u uint64) error {
		c := cid.NewCidV1(cid.Raw, mh)

		has, err := rbs.Has(ctx, c)
		require.NoError(t, err)
		require.True(t, has)

		blk, err := rbs.Get(ctx, c)
		require.NoError(t, err)
		require.NotEmpty(t, blk)

		sz, err := rbs.GetSize(ctx, c)
		require.NoError(t, err)
		require.EqualValues(t, len(blk.RawData()), sz)

		require.EqualValues(t, c, blk.Cid())
		return nil
	})

	// ------------------------------------------
	// Now test with a shard selector that rejects everything and ensure we always see errors
	fss := func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
		return shard.Key{}, errors.New("rejected")
	}

	rbs, err = NewIndexBackedBlockstore(dagst, fss, 10, 10)
	require.NoError(t, err)
	it.ForEach(func(mh multihash.Multihash, u uint64) error {
		c := cid.NewCidV1(cid.Raw, mh)

		has, err := rbs.Has(ctx, c)
		require.Error(t, err)
		require.False(t, has)

		blk, err := rbs.Get(ctx, c)
		require.Error(t, err)
		require.Empty(t, blk)

		sz, err := rbs.GetSize(ctx, c)
		require.Error(t, err)
		require.EqualValues(t, 0, sz)

		return nil
	})
}

func testRegistry(t *testing.T) *mount.Registry {
	r := mount.NewRegistry()
	err := r.Register("fs", &mount.FSMount{FS: testdata.FS})
	require.NoError(t, err)
	err = r.Register("counting", new(mount.Counting))
	require.NoError(t, err)
	return r
}

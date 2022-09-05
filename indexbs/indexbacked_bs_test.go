package indexbs

import (
	"context"
	"errors"
	"testing"

	"golang.org/x/sync/errgroup"

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

	rbs, err := NewIndexBackedBlockstore(dagst, noOpSelector, 10)
	require.NoError(t, err)

	// iterate over the CARV2 Index for the given CARv2 file and ensure the readonly blockstore
	// works for each of those cids
	it, err := dagst.GetIterableIndex(sk)
	require.NoError(t, err)

	var errg errgroup.Group

	it.ForEach(func(mh multihash.Multihash, _ uint64) error {

		mhs := mh
		errg.Go(func() error {
			c := cid.NewCidV1(cid.Raw, mhs)

			// Has
			has, err := rbs.Has(ctx, c)
			if err != nil {
				return err
			}
			if !has {
				return errors.New("has should be true")
			}

			// Get
			blk, err := rbs.Get(ctx, c)
			if err != nil {
				return err
			}
			if blk == nil {
				return errors.New("block should not be empty")
			}

			// GetSize
			_, err = rbs.GetSize(ctx, c)
			if err != nil {
				return err
			}

			// ensure cids match
			if blk.Cid() != c {
				return errors.New("cid mismatch")
			}
			return nil

		})

		return nil
	})

	require.NoError(t, errg.Wait())

	// ------------------------------------------
	// Test with a shard selector that returns an error and verify all methods
	// return the error
	rejectedErr := errors.New("rejected")
	fss := func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
		return shard.Key{}, rejectedErr
	}

	rbs, err = NewIndexBackedBlockstore(dagst, fss, 10)
	require.NoError(t, err)
	it.ForEach(func(mh multihash.Multihash, u uint64) error {
		c := cid.NewCidV1(cid.Raw, mh)

		has, err := rbs.Has(ctx, c)
		require.ErrorIs(t, err, rejectedErr)
		require.False(t, has)

		blk, err := rbs.Get(ctx, c)
		require.ErrorIs(t, err, rejectedErr)
		require.Empty(t, blk)

		sz, err := rbs.GetSize(ctx, c)
		require.ErrorIs(t, err, rejectedErr)
		require.EqualValues(t, 0, sz)

		return nil
	})

	// ------------------------------------------
	// Test with a shard selector that returns ErrNoShardSelected
	fss = func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
		return shard.Key{}, ErrNoShardSelected
	}

	rbs, err = NewIndexBackedBlockstore(dagst, fss, 10)
	require.NoError(t, err)
	it.ForEach(func(mh multihash.Multihash, u uint64) error {
		c := cid.NewCidV1(cid.Raw, mh)

		// Has should return false
		has, err := rbs.Has(ctx, c)
		require.NoError(t, err)
		require.False(t, has)

		// Get should return ErrBlockNotFound
		blk, err := rbs.Get(ctx, c)
		require.ErrorIs(t, err, ErrBlockNotFound)
		require.Empty(t, blk)

		// GetSize should return ErrBlockNotFound
		sz, err := rbs.GetSize(ctx, c)
		require.ErrorIs(t, err, ErrBlockNotFound)
		require.EqualValues(t, 0, sz)

		return nil
	})

	// ------------------------------------------
	// Test with a cid that isn't in the shard
	notFoundCid, err := cid.Parse("bafzbeigai3eoy2ccc7ybwjfz5r3rdxqrinwi4rwytly24tdbh6yk7zslrm")
	require.NoError(t, err)

	rbs, err = NewIndexBackedBlockstore(dagst, noOpSelector, 10)
	require.NoError(t, err)

	// Has should return false
	has, err := rbs.Has(ctx, notFoundCid)
	require.NoError(t, err)
	require.False(t, has)

	// Get should return ErrBlockNotFound
	blk, err := rbs.Get(ctx, notFoundCid)
	require.ErrorIs(t, err, ErrBlockNotFound)
	require.Empty(t, blk)

	// GetSize should return ErrBlockNotFound
	sz, err := rbs.GetSize(ctx, notFoundCid)
	require.ErrorIs(t, err, ErrBlockNotFound)
	require.EqualValues(t, 0, sz)
}

func testRegistry(t *testing.T) *mount.Registry {
	r := mount.NewRegistry()
	err := r.Register("fs", &mount.FSMount{FS: testdata.FS})
	require.NoError(t, err)
	err = r.Register("counting", new(mount.Counting))
	require.NoError(t, err)
	return r
}

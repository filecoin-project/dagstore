package dagstore

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"testing"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	carv1path = "testdata/sample-v1.car"
	carv2path = "testdata/sample-wrapped-v2.car"
)

var (
	//go:embed testdata/*
	testdata embed.FS

	carv1 []byte
	carv2 []byte

	// rootCID is the root CID of the carv2 for testing.
	rootCID cid.Cid
)

func init() {
	_ = logging.SetLogLevel("dagstore", "DEBUG")

	var err error
	carv1, err = testdata.ReadFile(carv1path)
	if err != nil {
		panic(err)
	}

	carv2, err = testdata.ReadFile(carv2path)
	if err != nil {
		panic(err)
	}

	reader, err := car.NewReader(bytes.NewReader(carv2))
	if err != nil {
		panic(fmt.Errorf("failed to parse carv2: %w", err))
	}
	defer reader.Close()

	roots, err := reader.Roots()
	if err != nil {
		panic(fmt.Errorf("failed to obtain carv2 roots: %w", err))
	}
	if len(roots) == 0 {
		panic("carv2 has no roots")
	}
	rootCID = roots[0]
}

func TestRegisterCarV1(t *testing.T) {
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv1path}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.Error(t, res.Error)
	require.Contains(t, res.Error.Error(), "invalid car version")
	require.EqualValues(t, k, res.Key)
	require.Nil(t, res.Accessor)

	info := dagst.AllShardsInfo()
	require.Len(t, info, 1)
	for _, ss := range info {
		require.Equal(t, ShardStateErrored, ss.ShardState)
		require.Error(t, ss.Error)
	}
}

func TestRegisterCarV2(t *testing.T) {
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv2path}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)
	require.EqualValues(t, k, res.Key)
	require.Nil(t, res.Accessor)

	info := dagst.AllShardsInfo()
	require.Len(t, info, 1)
	for _, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
	}
}

func TestRegisterConcurrentShards(t *testing.T) {
	run := func(t *testing.T, n int) {
		store := dssync.MutexWrap(datastore.NewMapDatastore())
		dagst, err := NewDAGStore(Config{
			MountRegistry: testRegistry(t),
			TransientsDir: t.TempDir(),
			Datastore:     store,
		})
		require.NoError(t, err)

		registerShards(t, dagst, n)
	}

	t.Run("1", func(t *testing.T) { run(t, 1) })
	t.Run("2", func(t *testing.T) { run(t, 2) })
	t.Run("4", func(t *testing.T) { run(t, 4) })
	t.Run("8", func(t *testing.T) { run(t, 8) })
	t.Run("16", func(t *testing.T) { run(t, 16) })
	t.Run("32", func(t *testing.T) { run(t, 32) })
	t.Run("64", func(t *testing.T) { run(t, 64) })
	t.Run("128", func(t *testing.T) { run(t, 128) })
	t.Run("256", func(t *testing.T) { run(t, 256) })
}

func TestAcquireInexistentShard(t *testing.T) {
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.Error(t, err)
}

func TestAcquireAfterRegisterWait(t *testing.T) {
	t.Skip("uncomment when https://github.com/ipfs/go-cid/issues/126#issuecomment-872364155 is fixed")
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv2path}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)

	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.NoError(t, err)

	res = <-ch
	require.NoError(t, res.Error)
	require.NotNil(t, res.Accessor)
	require.EqualValues(t, k, res.Accessor.Shard())

	bs, err := res.Accessor.Blockstore()
	require.NoError(t, err)

	keys, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	for k := range keys {
		fmt.Println(k)
	}

	err = res.Accessor.Close()
	require.NoError(t, err)
}

func TestConcurrentAcquires(t *testing.T) {
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv2path}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)

	t.Run("1", func(t *testing.T) { acquireShard(t, dagst, k, 1) })
	t.Run("2", func(t *testing.T) { acquireShard(t, dagst, k, 2) })
	t.Run("4", func(t *testing.T) { acquireShard(t, dagst, k, 4) })
	t.Run("8", func(t *testing.T) { acquireShard(t, dagst, k, 8) })
	t.Run("16", func(t *testing.T) { acquireShard(t, dagst, k, 16) })
	t.Run("32", func(t *testing.T) { acquireShard(t, dagst, k, 32) })
	t.Run("64", func(t *testing.T) { acquireShard(t, dagst, k, 64) })
	t.Run("128", func(t *testing.T) { acquireShard(t, dagst, k, 128) })
	t.Run("256", func(t *testing.T) { acquireShard(t, dagst, k, 256) })

	info := dagst.AllShardsInfo()
	require.Len(t, info, 1)
	for _, ss := range info {
		require.Equal(t, ShardStateServing, ss.ShardState)
		require.NoError(t, ss.Error)
	}
}

func TestRestartRestoresState(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
	})
	require.NoError(t, err)

	keys := registerShards(t, dagst, 100)
	for _, k := range keys[0:20] { // acquire the first 20 keys.
		acquireShard(t, dagst, k, 4)
	}

	res, err := store.Query(dsq.Query{})
	require.NoError(t, err)
	entries, err := res.Rest()
	require.NoError(t, err)
	require.Len(t, entries, 100) // we have 100 shards.

	// close the DAG store.
	err = dagst.Close()
	require.NoError(t, err)

	// create a new dagstore with the same datastore.
	dagst, err = NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
	})
	require.NoError(t, err)
	info := dagst.AllShardsInfo()
	require.Len(t, info, 100)
	for _, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
	}
}

func TestRestartResumesRegistration(t *testing.T) {
	t.Skip("TODO")
}

// TestBlockCallback tests that blocking a callback blocks the dispatcher
// but not the event loop.
func TestBlockCallback(t *testing.T) {
	t.Skip("TODO")
}

// registerShards registers n shards concurrently, using the CARv2 mount.
func registerShards(t *testing.T, dagst *DAGStore, n int) (ret []shard.Key) {
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		k := shard.KeyFromString(fmt.Sprintf("shard-%d", i))
		grp.Go(func() error {
			ch := make(chan ShardResult, 1)
			err := dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv2path}, ch, RegisterOpts{})
			if err != nil {
				return err
			}
			res := <-ch
			return res.Error
		})
		ret = append(ret, k)
	}

	require.NoError(t, grp.Wait())

	info := dagst.AllShardsInfo()
	require.Len(t, info, n)
	for _, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
	}
	return ret
}

// acquireShard acquires the shard known by key `k` concurrently `n` times.
func acquireShard(t *testing.T, dagst *DAGStore, k shard.Key, n int) {
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		grp.Go(func() error {
			ch := make(chan ShardResult, 1)
			err := dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
			if err != nil {
				return err
			}

			res := <-ch
			if res.Error != nil {
				return res.Error
			}
			defer res.Accessor.Close()

			bs, err := res.Accessor.Blockstore()
			if err != nil {
				return err
			}

			_, err = bs.Get(rootCID)
			return err
		})
	}

	require.NoError(t, grp.Wait())
}

func testRegistry(t *testing.T) *mount.Registry {
	r := mount.NewRegistry()
	err := r.Register("fs", &mount.FSMount{FS: testdata})
	require.NoError(t, err)
	return r
}

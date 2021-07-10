package dagstore

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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

func TestRegisterUsingExistingTransient(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     ds,
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	// even though the fs mount has an empty path, the existing transient will get us through registration.
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: ""}, ch, RegisterOpts{ExistingTransient: carv2path})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)
	require.EqualValues(t, k, res.Key)
	require.Nil(t, res.Accessor)
	idx, err := dagst.indices.GetFullIndex(k)
	require.NoError(t, err)
	require.NotNil(t, idx)
}

func TestRegisterCarV1(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     ds,
	})
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv1path}, ch, RegisterOpts{})
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

	// verify index has been persisted
	istat, err := dagst.indices.StatFullIndex(k)
	require.NoError(t, err)
	require.True(t, istat.Exists)
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
	istat, err := dagst.indices.StatFullIndex(k)
	require.NoError(t, err)
	require.True(t, istat.Exists)
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

		registerShards(t, dagst, n, &mount.FSMount{FS: testdata, Path: carv2path})
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

	t.Run("1", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 1) })
	t.Run("2", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 2) })
	t.Run("4", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 4) })
	t.Run("8", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 8) })
	t.Run("16", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 16) })
	t.Run("32", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 32) })
	t.Run("64", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 64) })
	t.Run("128", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 128) })
	t.Run("256", func(t *testing.T) { acquireShardThenRelease(t, dagst, k, 256) })

	info := dagst.AllShardsInfo()
	require.Len(t, info, 1)
	for _, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
	}
}

func TestRestartRestoresState(t *testing.T) {
	indicesDir := t.TempDir()

	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
		IndexDir:      indicesDir,
	})
	require.NoError(t, err)

	keys := registerShards(t, dagst, 100, &mount.FSMount{FS: testdata, Path: carv2path})
	for _, k := range keys[0:20] { // acquire the first 20 keys.
		acquireShardThenRelease(t, dagst, k, 4)
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
		IndexDir:      indicesDir,
	})
	require.NoError(t, err)
	info := dagst.AllShardsInfo()
	require.Len(t, info, 100)

	for k, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
		require.Zero(t, ss.refs)

		// also ensure we have indices for all the shards.
		idx, err := dagst.indices.GetFullIndex(k)
		require.NoError(t, err)
		require.NotNil(t, idx)

		// ensure we can acquire the shard again
		acquireShardThenRelease(t, dagst, k, 10)

		// ensure we can't register the shard again
		err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata, Path: carv2path}, nil, RegisterOpts{})
		require.Error(t, err)
		require.Contains(t, err.Error(), ErrShardExists.Error())
	}
}

type blockingMount struct {
	mount.Mount
	ShouldBlock bool
}

func (b *blockingMount) Fetch(ctx context.Context) (mount.Reader, error) {
	if b.ShouldBlock {
		for {

		}
	} else {
		return b.Mount.Fetch(ctx)
	}
}

func TestRestartResumesRegistration(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	rg := testRegistry(t)
	require.NoError(t, rg.Register("block", &blockingMount{&mount.FSMount{FS: testdata}, true}))

	dagst, err := NewDAGStore(Config{
		MountRegistry: rg,
		TransientsDir: dir,
		Datastore:     store,
	})
	require.NoError(t, err)

	// start registering a shard -> registration will not complete as mount.Fetch will hang.
	k := shard.KeyFromString("test")
	ch := make(chan ShardResult, 1)
	err = dagst.RegisterShard(context.Background(), k, &blockingMount{&mount.FSMount{FS: testdata, Path: carv2path}, true}, ch, RegisterOpts{})
	require.NoError(t, err)

	// fetch the shard state
	info := flushAndGetShardState(t, dagst, k)
	require.EqualValues(t, ShardStateInitializing, info.ShardState)

	// close the dagstore
	require.NoError(t, dagst.Close())
	require.NoError(t, os.RemoveAll(dir))

	// start a new DAGStore and do not block the fetch this time -> registration should work.
	// create a new dagstore with the same datastore.
	rg = testRegistry(t)
	require.NoError(t, rg.Register("block", &blockingMount{&mount.FSMount{FS: testdata}, false}))
	dagst, err = NewDAGStore(Config{
		MountRegistry: rg,
		TransientsDir: dir,
		Datastore:     store,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		info = flushAndGetShardState(t, dagst, k)
		return info.ShardState == ShardStateAvailable
	}, 5*time.Second, 2*time.Second)

	info = flushAndGetShardState(t, dagst, k)
	require.EqualValues(t, ShardStateAvailable, info.ShardState)

	// ensure we can acquire the shard
	acquireShardThenRelease(t, dagst, k, 10)
	// ensure index exists for the shard
	idx, err := dagst.indices.GetFullIndex(k)
	require.NoError(t, err)
	require.NotNil(t, idx)
}

// TestBlockCallback tests that blocking a callback blocks the dispatcher
// but not the event loop.
func TestBlockCallback(t *testing.T) {
	t.Skip("TODO")
}

// registerShards registers n shards concurrently, using the CARv2 mount.
func registerShards(t *testing.T, dagst *DAGStore, n int, mnt mount.Mount) (ret []shard.Key) {
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		k := shard.KeyFromString(fmt.Sprintf("shard-%d", i))
		grp.Go(func() error {
			ch := make(chan ShardResult, 1)
			err := dagst.RegisterShard(context.Background(), k, mnt, ch, RegisterOpts{})
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
	for k, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
		istat, err := dagst.indices.StatFullIndex(k)
		require.NoError(t, err)
		require.True(t, istat.Exists)
	}

	return ret
}

// acquireShardThenRelease first acquires the shard known by key `k` concurrently `n` times.
// then closes all the accessors and releases the shards.
func acquireShardThenRelease(t *testing.T, dagst *DAGStore, k shard.Key, n int) {
	var mu sync.RWMutex
	accessors := make([]*ShardAccessor, 0, n)

	// acquire
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

			bs, err := res.Accessor.Blockstore()
			if err != nil {
				return err
			}

			if _, err := bs.Get(rootCID); err != nil {
				return err
			}

			mu.Lock()
			accessors = append(accessors, res.Accessor)
			mu.Unlock()
			return nil
		})
	}

	require.NoError(t, grp.Wait())
	info := flushAndGetShardState(t, dagst, k)
	require.Equal(t, ShardStateAvailable, info.ShardState)
	require.NoError(t, info.Error)
	// refs should be equal to number of acquirers since we've not closed any acquirer/released any shard.
	require.EqualValues(t, n, info.refs)
	// ensure transient file path exists.
	abs := filepath.Join(dagst.config.TransientsDir, k.String())
	_, err := os.Stat(abs)
	require.NoError(t, err)

	// close all the accessors
	grp2, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		ac := accessors[i]
		grp2.Go(func() error {
			mu.RLock()
			defer mu.RUnlock()
			if err := ac.Close(); err != nil {
				return err
			}
			return nil
		})
	}

	require.NoError(t, grp2.Wait())
	info = flushAndGetShardState(t, dagst, k)
	require.Equal(t, ShardStateAvailable, info.ShardState)
	require.NoError(t, info.Error)
	// refs should be zero now since shard accessors have been closed and transient file should be cleaned up.
	require.Zero(t, info.refs)
	_, err = os.Stat(abs)
	require.Error(t, err)
}

func flushAndGetShardState(t *testing.T, dagst *DAGStore, key shard.Key) ShardInfo {
	ch := make(chan ShardResult, 1)
	// wait for all existing ops for the shard to be processed by the event loop.
	require.NoError(t, dagst.flush(context.Background(), key, ch))
	<-ch
	info, err := dagst.GetShardInfo(key)
	require.NoError(t, err)
	return info
}

func testRegistry(t *testing.T) *mount.Registry {
	r := mount.NewRegistry()
	err := r.Register("fs", &mount.FSMount{FS: testdata})
	require.NoError(t, err)
	return r
}

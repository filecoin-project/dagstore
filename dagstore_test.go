package dagstore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/multiformats/go-multihash"

	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/dagstore/testdata"
)

var (
	carv2mnt = &mount.FSMount{FS: testdata.FS, Path: testdata.FSPathCarV2}
	junkmnt  = &mount.FSMount{FS: testdata.FS, Path: testdata.FSPathJunk}
)

func init() {
	_ = logging.SetLogLevel("dagstore", "DEBUG")
}

// TestDestroyShard tests that shards are removed properly and relevant
// errors are returned in case of failure.
func TestDestroyShard(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	sink := tracer(128)
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
		TraceCh:       sink,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	sh := []string{"foo", "bar"}

	for _, v := range sh {
		ch := make(chan ShardResult, 1)
		k := shard.KeyFromString(v)
		counting := &mount.Counting{Mount: carv2mnt}
		err = dagst.RegisterShard(context.Background(), k, counting, ch, RegisterOpts{
			LazyInitialization: false,
		})
		require.NoError(t, err)
		res1 := <-ch
		require.NoError(t, res1.Error)

		info, err := dagst.GetShardInfo(k)
		require.NoError(t, err)
		require.Equal(t, ShardStateAvailable, info.ShardState)
	}

	// Acquire one shard and keep other Available
	acres := make(chan ShardResult, 1)

	err = dagst.AcquireShard(context.Background(), shard.KeyFromString(sh[0]), acres, AcquireOpts{})
	require.NoError(t, err)

	res := <-acres
	require.NoError(t, res.Error)

	// Try to destroy both shards, fail for any error except Active Shards
	desres1 := make(chan ShardResult, 1)
	err = dagst.DestroyShard(context.Background(), shard.KeyFromString(sh[0]), desres1, DestroyOpts{})
	require.NoError(t, err)
	res1 := <-desres1
	require.Contains(t, res1.Error.Error(), "failed to destroy shard; active references")

	desres2 := make(chan ShardResult, 1)
	err = dagst.DestroyShard(context.Background(), shard.KeyFromString(sh[1]), desres2, DestroyOpts{})
	require.NoError(t, err)
	res2 := <-desres2
	require.NoError(t, res2.Error)

	info := dagst.AllShardsInfo()
	require.Equal(t, 1, len(info))
}

func TestDestroyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	idx, err := index.NewFSRepo(t.TempDir())
	require.NoError(t, err)

	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	keys := registerShards(t, dagst, 100, carv2mnt, RegisterOpts{})

	res, err := store.Query(context.TODO(), dsq.Query{})
	require.NoError(t, err)
	entries, err := res.Rest()
	require.NoError(t, err)
	require.Len(t, entries, 100) // we have 100 shards.

	// Remove 20 shards
	for _, j := range keys[20:40] {
		desres := make(chan ShardResult, 1)
		err = dagst.DestroyShard(context.Background(), j, desres, DestroyOpts{})
		require.NoError(t, err)
		res := <-desres
		require.NoError(t, res.Error)
	}

	// close the DAG store.
	err = dagst.Close()
	require.NoError(t, err)

	// create a new dagstore with the same datastore.
	dagst, err = NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// Compare the number of shards after restart
	info := dagst.AllShardsInfo()
	l := len(info)
	t.Log(l)
	require.Len(t, info, 80)
}

func TestRegisterUsingExistingTransient(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     ds,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	// even though the fs mount has an empty path, the existing transient will get us through registration.
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata.FS, Path: ""}, ch, RegisterOpts{ExistingTransient: testdata.RootPathCarV2})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)
	require.EqualValues(t, k, res.Key)
	require.Nil(t, res.Accessor)
	idx, err := dagst.indices.GetFullIndex(k)
	require.NoError(t, err)
	require.NotNil(t, idx)
}

func TestRegisterWithaNilResponseChannel(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     ds,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	k := shard.KeyFromString("foo")
	// we pass a nil response channel to Register Shard here
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata.FS, Path: testdata.FSPathCarV1}, nil, RegisterOpts{})
	require.NoError(t, err)

	// acquire and wait for acquire
	ch := make(chan ShardResult, 1)
	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)
	require.NotNil(t, res.Accessor)
	require.EqualValues(t, k, res.Accessor.Shard())
	err = res.Accessor.Close()
	require.NoError(t, err)

	// verify index has been persisted
	istat, err := dagst.indices.StatFullIndex(k)
	require.NoError(t, err)
	require.True(t, istat.Exists)
}

func TestRegisterCarV1(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     ds,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata.FS, Path: testdata.FSPathCarV1}, ch, RegisterOpts{})
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
	ctx := context.Background()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, carv2mnt, ch, RegisterOpts{})
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

	// ensure it's registered with the correct iterable index and has been indexed in the top-level index
	ii, err := dagst.GetIterableIndex(k)
	require.NoError(t, err)
	require.NotNil(t, ii)
	err = ii.ForEach(func(h multihash.Multihash, _ uint64) error {
		k2, err := dagst.ShardsContainingMultihash(ctx, h)
		if err != nil {
			return err
		}

		if len(k2) != 1 {
			return errors.New("should match only one pieceCid")
		}

		if k2[0] != k {
			return errors.New("not the correct key")
		}
		return nil
	})
	require.NoError(t, err)
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

		err = dagst.Start(context.Background())
		require.NoError(t, err)

		registerShards(t, dagst, n, carv2mnt, RegisterOpts{})
	}

	t.Run("1", func(t *testing.T) { run(t, 1) })
	t.Run("2", func(t *testing.T) { run(t, 2) })
	t.Run("4", func(t *testing.T) { run(t, 4) })
	t.Run("8", func(t *testing.T) { run(t, 8) })
	t.Run("16", func(t *testing.T) { run(t, 16) })
	t.Run("32", func(t *testing.T) { run(t, 32) })
}

func TestAcquireInexistentShard(t *testing.T) {
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: t.TempDir(),
		Datastore:     datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
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

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, carv2mnt, ch, RegisterOpts{})
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

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, carv2mnt, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)

	// the test consists of acquiring then releasing.
	run := func(t *testing.T, n int) {
		accessors := acquireShard(t, dagst, k, n)
		releaseAll(t, dagst, k, accessors)
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

	info := dagst.AllShardsInfo()
	require.Len(t, info, 1)
	for _, ss := range info {
		require.Equal(t, ShardStateAvailable, ss.ShardState)
		require.NoError(t, ss.Error)
	}
}

func TestRestartRestoresState(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	idx, err := index.NewFSRepo(t.TempDir())
	require.NoError(t, err)
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	keys := registerShards(t, dagst, 100, carv2mnt, RegisterOpts{})
	for _, k := range keys[0:20] { // acquire the first 20 keys.
		_ = acquireShard(t, dagst, k, 4)
	}

	res, err := store.Query(context.TODO(), dsq.Query{})
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
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
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
		_ = acquireShard(t, dagst, k, 10)

		// ensure we can't register the shard again
		err = dagst.RegisterShard(context.Background(), k, carv2mnt, nil, RegisterOpts{})
		require.Error(t, err)
		require.Contains(t, err.Error(), ErrShardExists.Error())
	}
}

func TestRestartResumesRegistration(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	r := testRegistry(t)

	err := r.Register("block", newBlockingMount(&mount.FSMount{FS: testdata.FS}))
	require.NoError(t, err)

	sink := tracer(128)
	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		Datastore:     store,
		TraceCh:       sink,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// start registering a shard -> registration will not complete as mount.Fetch will hang.
	k := shard.KeyFromString("test")
	ch := make(chan ShardResult, 1)
	block := newBlockingMount(carv2mnt)
	err = dagst.RegisterShard(context.Background(), k, block, ch, RegisterOpts{})
	require.NoError(t, err)

	// receive at most two traces in 1 second.
	traces := make([]Trace, 16)
	n, timedOut := sink.Read(traces, 1*time.Second)
	require.Equal(t, 2, n)
	require.True(t, timedOut)

	// no OpMakeAvailable trace; shard state is initializing.
	require.Equal(t, OpShardRegister, traces[0].Op)
	require.Equal(t, ShardStateNew, traces[0].After.ShardState)

	require.Equal(t, OpShardInitialize, traces[1].Op)
	require.Equal(t, ShardStateInitializing, traces[1].After.ShardState)

	// corroborate we see the same through the API.
	info, err := dagst.GetShardInfo(k)
	require.NoError(t, err)
	require.EqualValues(t, ShardStateInitializing, info.ShardState)

	t.Log("closing")

	// close the dagstore and remove the transients.
	err = dagst.Close()
	require.NoError(t, err)

	// start a new DAGStore and do not block the fetch this time -> registration should work.
	// create a new dagstore with the same datastore.
	//
	// Instantiate a new registry using a blocking mount that we control as
	// a template. Because UnblockCh is exported, it is a templated field, so
	// all mounts will await for tokens on that shared channel.
	r = testRegistry(t)
	bm := newBlockingMount(&mount.FSMount{FS: testdata.FS})
	err = r.Register("block", bm)
	require.NoError(t, err)

	// unblock the mount this time!
	bm.UnblockNext(1)

	dagst, err = NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		Datastore:     store,
		TraceCh:       sink,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// this time we will receive three traces; OpShardInitialize, and OpShardMakeAvailable.
	n, timedOut = sink.Read(traces, 1*time.Second)
	require.Equal(t, 3, n)
	require.True(t, timedOut)

	// trace 1.
	require.Equal(t, OpShardRegister, traces[0].Op)
	require.Equal(t, ShardStateNew, traces[0].After.ShardState)

	// trace 2.
	require.Equal(t, OpShardInitialize, traces[1].Op)
	require.Equal(t, ShardStateInitializing, traces[1].After.ShardState)

	// trace 3.
	require.Equal(t, OpShardMakeAvailable, traces[2].Op)
	require.Equal(t, ShardStateAvailable, traces[2].After.ShardState)

	// ensure we have indices.
	idx, err := dagst.indices.GetFullIndex(k)
	require.NoError(t, err)
	require.NotNil(t, idx)

	// now let's acquire the shard, and ensure we receive two more traces.
	// one for OpAcquire, one for OpRelease.
	accessors := acquireShard(t, dagst, k, 1)
	releaseAll(t, dagst, k, accessors)

	n, timedOut = sink.Read(traces, 1*time.Second)
	require.Equal(t, 2, n)
	require.True(t, timedOut)

	// trace 1.
	require.Equal(t, OpShardAcquire, traces[0].Op)
	require.Equal(t, ShardStateServing, traces[0].After.ShardState)

	// trace 2.
	require.Equal(t, OpShardRelease, traces[1].Op)
	require.Equal(t, ShardStateAvailable, traces[1].After.ShardState)
}

func TestGC(t *testing.T) {
	dir := t.TempDir()
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 100 shards
	// acquire 25 with 5 acquirers, release 2 acquirers (refcount 3); non reclaimable
	// acquire another 25, release them all, they're reclaimable
	shards := registerShards(t, dagst, 100, carv2mnt, RegisterOpts{})
	for _, k := range shards[0:25] {
		accessors := acquireShard(t, dagst, k, 5)
		for _, acc := range accessors[:2] {
			err := acc.Close()
			require.NoError(t, err)
		}
	}
	for _, k := range shards[25:50] {
		accessors := acquireShard(t, dagst, k, 5)
		releaseAll(t, dagst, k, accessors)
	}

	results, err := dagst.GC(context.Background())
	require.NoError(t, err)
	require.Len(t, results.Shards, 75) // all but the second batch of 25 have been reclaimed.
	require.Zero(t, results.ShardFailures())

	for i := 25; i < 100; i++ {
		k := shard.KeyFromString(fmt.Sprintf("shard-%d", i))
		err, ok := results.Shards[k]
		require.True(t, ok)
		require.NoError(t, err)
	}
}

func TestOrphansRemovedOnStartup(t *testing.T) {
	dir := t.TempDir()

	// create random files in the transients directory, which we expect the GC
	// procedure to remove.
	var orphaned []string
	for i := 0; i < 100; i++ {
		file, err := os.CreateTemp(dir, "")
		require.NoError(t, err)
		orphaned = append(orphaned, file.Name())

		// write random data
		n, err := io.Copy(file, io.LimitReader(rand.Reader, 1024))
		require.NoError(t, err)
		require.EqualValues(t, 1024, n)

		err = file.Close()
		require.NoError(t, err)
	}

	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
	})
	require.NoError(t, err)
	defer dagst.Close()

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// orphaned files are gone
	for _, p := range orphaned {
		_, err := os.Stat(p)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

// TestLazyInitialization tests that lazy initialization initializes shards on
// their first acquisition.
func TestLazyInitialization(t *testing.T) {
	dir := t.TempDir()
	store := datastore.NewLogDatastore(dssync.MutexWrap(datastore.NewMapDatastore()), "trace")
	sink := tracer(128)
	dagst, err := NewDAGStore(Config{
		MountRegistry: testRegistry(t),
		TransientsDir: dir,
		Datastore:     store,
		TraceCh:       sink,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	counting := &mount.Counting{Mount: carv2mnt}
	err = dagst.RegisterShard(context.Background(), k, counting, ch, RegisterOpts{
		LazyInitialization: true,
	})
	require.NoError(t, err)
	res := <-ch
	require.NoError(t, res.Error)

	info, err := dagst.GetShardInfo(k)
	require.NoError(t, err)
	require.Equal(t, ShardStateNew, info.ShardState)

	// we haven't tried to fetch the resource.
	require.Zero(t, counting.Count())

	t.Log("now acquiring")

	// do 16 simultaneous acquires.
	acquireShard(t, dagst, k, 16)

	// verify that we've fetched the shard only once.
	require.Equal(t, 1, counting.Count())

	info, err = dagst.GetShardInfo(k)
	require.NoError(t, err)
	require.Equal(t, ShardStateServing, info.ShardState)
	require.EqualValues(t, 16, info.refs)
}

// TestThrottleFetch exercises and tests the fetch concurrency limitation.
// Testing thottling on indexing is way harder...
func TestThrottleFetch(t *testing.T) {
	r := testRegistry(t)
	err := r.Register("block", newBlockingMount(&mount.FSMount{FS: testdata.FS}))
	require.NoError(t, err)

	dir := t.TempDir()
	sink := tracer(128)
	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		TraceCh:       sink,

		MaxConcurrentReadyFetches: 5,
		MaxConcurrentIndex:        5,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 16 shards with lazy init, against the blocking mount.
	// we don't register with eager, because we would block due to the throttle.
	mnt := newBlockingMount(carv2mnt)
	mnt.ready = true
	cnt := &mount.Counting{Mount: mnt}
	resCh := make(chan ShardResult, 16)
	for i := 0; i < 16; i++ {
		k := shard.KeyFromString(strconv.Itoa(i))
		err := dagst.RegisterShard(context.Background(), k, cnt, resCh, RegisterOpts{})
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	info := dagst.AllShardsInfo()
	require.Len(t, info, 16)
	for _, i := range info {
		require.Equal(t, ShardStateInitializing, i.ShardState)
	}

	// no responses received.
	require.Len(t, resCh, 0)

	// mount was called 5 times only.
	require.EqualValues(t, 5, cnt.Count())

	// allow 5 to proceed; those will be initialized an the next 5 will block.
	mnt.UnblockNext(5)
	time.Sleep(500 * time.Millisecond)

	// five responses received.
	require.Len(t, resCh, 5)

	// mount was called another 5 times.
	require.EqualValues(t, 10, cnt.Count())

	info = dagst.AllShardsInfo()
	require.Len(t, info, 16)

	m := map[ShardState][]shard.Key{}
	for k, i := range info {
		m[i.ShardState] = append(m[i.ShardState], k)
	}
	require.Len(t, m, 2) // only two shard states.
	require.Len(t, m[ShardStateInitializing], 16-5)
	require.Len(t, m[ShardStateAvailable], 5)
}

func TestIndexingFailure(t *testing.T) {
	r := testRegistry(t)
	dir := t.TempDir()
	sink := tracer(128)
	failures := make(chan ShardResult, 128)
	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		TraceCh:       sink,
		FailureCh:     failures,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 16 shards with junk in them, so they will fail indexing.
	resCh := make(chan ShardResult, 16)
	junkmnt := *junkmnt // take a copy
	for i := 0; i < 16; i++ {
		k := shard.KeyFromString(strconv.Itoa(i))
		err := dagst.RegisterShard(context.Background(), k, &junkmnt, resCh, RegisterOpts{})
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	// 16 failures were notified.
	require.Len(t, failures, 16)
	for i := 0; i < 16; i++ {
		f := <-failures
		require.Error(t, f.Error)

		res := <-resCh
		require.Error(t, res.Error)
	}

	info := dagst.AllShardsInfo()
	for _, i := range info {
		require.Equal(t, ShardStateErrored, i.ShardState)
		require.Error(t, i.Error)
	}

	evts := make([]Trace, 48)
	n, timedOut := sink.Read(evts, 50*time.Millisecond)
	require.False(t, timedOut)
	require.Equal(t, 48, n)

	// first 48 events are OpShardRegister, OpShardInitialize, OpShardFail.
	for i := 0; i < 48; i++ {
		evt := evts[i]
		switch evt.Op {
		case OpShardRegister:
			require.EqualValues(t, ShardStateNew, evt.After.ShardState)
			require.NoError(t, evt.After.Error)
		case OpShardInitialize:
			require.EqualValues(t, ShardStateInitializing, evt.After.ShardState)
			require.NoError(t, evt.After.Error)
		case OpShardFail:
			require.EqualValues(t, ShardStateErrored, evt.After.ShardState)
			require.Error(t, evt.After.Error)
		default:
			t.Fatalf("unexpected op: %s", evt.Op)
		}
	}

	t.Run("fails again", func(t *testing.T) {
		// continue using a bad path.
		junkmnt.Path = testdata.FSPathJunk

		// try to recover, it will fail again.
		for i := 0; i < 16; i++ {
			k := shard.KeyFromString(strconv.Itoa(i))
			err := dagst.RecoverShard(context.Background(), k, resCh, RecoverOpts{})
			require.NoError(t, err)
		}

		time.Sleep(500 * time.Millisecond)

		// 16 failures were notified.
		require.Len(t, failures, 16)
		for i := 0; i < 16; i++ {
			f := <-failures
			require.Error(t, f.Error)

			res := <-resCh
			require.Error(t, res.Error)
		}

		info := dagst.AllShardsInfo()
		for k, i := range info {
			require.Equal(t, ShardStateErrored, i.ShardState)
			require.Error(t, i.Error)

			// no index
			istat, err := dagst.indices.StatFullIndex(k)
			require.NoError(t, err)
			require.False(t, istat.Exists)
		}

		// verify that all acquires fail immediately.
		for k := range dagst.AllShardsInfo() {
			ch := make(chan ShardResult)
			err := dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
			require.NoError(t, err)
			res := <-ch
			require.Error(t, res.Error)
			require.Equal(t, k, res.Key)
			require.Nil(t, res.Accessor)
		}

		evts := make([]Trace, 48)
		n, timedOut := sink.Read(evts, 50*time.Millisecond)
		require.False(t, timedOut)
		require.Equal(t, 48, n)

		// these 48 traces are OpShardRecover, OpShardFail, OpShardAcquire.
		for i := 0; i < 48; i++ {
			evt := evts[i]
			switch evt.Op {
			case OpShardRecover:
				require.EqualValues(t, ShardStateRecovering, evt.After.ShardState)
				require.Error(t, evt.After.Error)
			case OpShardFail:
				require.EqualValues(t, ShardStateErrored, evt.After.ShardState)
				require.Error(t, evt.After.Error)
			case OpShardAcquire:
				require.EqualValues(t, ShardStateErrored, evt.After.ShardState)
				require.Error(t, evt.After.Error)
			default:
				t.Fatalf("unexpected op: %s", evt.Op)
			}
		}

	})

	t.Run("recovers", func(t *testing.T) {
		// use a good path.
		junkmnt.Path = testdata.FSPathCarV2

		// try to recover, it will succeed.
		for i := 0; i < 16; i++ {
			k := shard.KeyFromString(strconv.Itoa(i))
			err := dagst.RecoverShard(context.Background(), k, resCh, RecoverOpts{})
			require.NoError(t, err)
		}

		// no need to wait, since the recovery channel will fire when the shard
		// is actually recovered.
		// time.Sleep(500 * time.Millisecond)

		// 0 failures were notified.
		require.Len(t, failures, 0)

		for i := 0; i < 16; i++ {
			res := <-resCh
			require.NoError(t, res.Error)
		}

		info := dagst.AllShardsInfo()
		for k, i := range info {
			require.Equal(t, ShardStateAvailable, i.ShardState)
			require.NoError(t, i.Error)

			// an index exists!
			istat, err := dagst.indices.StatFullIndex(k)
			require.NoError(t, err)
			require.True(t, istat.Exists)
		}

		// verify that all acquires succeed now.
		for k := range dagst.AllShardsInfo() {
			ch := make(chan ShardResult)
			err := dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
			require.NoError(t, err)

			res := <-ch
			require.NoError(t, res.Error)
			require.Equal(t, k, res.Key)
			require.NotNil(t, res.Accessor)
		}

		evts := make([]Trace, 48)
		n, timedOut := sink.Read(evts, 50*time.Millisecond)
		require.False(t, timedOut)
		require.Equal(t, 48, n)

		// these 48 traces are OpShardRecover, OpShardFail.
		for i := 0; i < 48; i++ {
			evt := evts[i]
			switch evt.Op {
			case OpShardRecover:
				require.EqualValues(t, ShardStateRecovering, evt.After.ShardState)
				require.Error(t, evt.After.Error)
			case OpShardMakeAvailable:
				require.EqualValues(t, ShardStateAvailable, evt.After.ShardState)
				require.NoError(t, evt.After.Error)
			case OpShardAcquire:
				require.EqualValues(t, ShardStateServing, evt.After.ShardState)
				require.NoError(t, evt.After.Error)
			default:
				t.Fatalf("unexpected op: %s", evt.Op)
			}
		}
	})
}

func TestFailureRecovery(t *testing.T) {
	r := testRegistry(t)
	dir := t.TempDir()
	sink := tracer(128)
	failures := make(chan ShardResult, 128)
	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		TraceCh:       sink,
		FailureCh:     failures,
	})

	go RecoverImmediately(context.Background(), dagst, failures, 10, nil) // 10 max attempts.
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 16 shards with junk in them, so they will fail indexing.
	resCh := make(chan ShardResult, 16)
	junkmnt := *junkmnt // take a copy
	for i := 0; i < 16; i++ {
		k := shard.KeyFromString(strconv.Itoa(i))
		err := dagst.RegisterShard(context.Background(), k, &junkmnt, resCh, RegisterOpts{})
		require.NoError(t, err)
	}

	evts := make([]Trace, 368)
	n, timedOut := sink.Read(evts, 5*time.Second)
	require.False(t, timedOut)
	require.Equal(t, 368, n)

	counts := map[OpType]int{}
	for _, evt := range evts {
		counts[evt.Op]++

		switch evt.Op {
		case OpShardRecover:
			require.EqualValues(t, ShardStateRecovering, evt.After.ShardState)
			require.Error(t, evt.After.Error)
		case OpShardFail:
			require.EqualValues(t, ShardStateErrored, evt.After.ShardState)
			require.Error(t, evt.After.Error)
		}
	}

	require.Equal(t, counts[OpShardRegister], 16)
	require.Equal(t, counts[OpShardInitialize], 16)
	require.Equal(t, counts[OpShardFail], 176)
	require.Equal(t, counts[OpShardRecover], 160)
}

func TestRecoveryOnStart(t *testing.T) {
	// populate a few failing shards.
	ds := datastore.NewMapDatastore()
	r := testRegistry(t)
	dir := t.TempDir()
	sink := tracer(128)
	failures := make(chan ShardResult, 128)
	config := Config{
		MountRegistry: r,
		TransientsDir: dir,
		TraceCh:       sink,
		FailureCh:     failures,
		Datastore:     ds,
	}
	dagst, err := NewDAGStore(config)
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 16 shards with junk in them, so they will fail indexing.
	resCh := make(chan ShardResult, 16)
	junkmnt := *junkmnt // take a copy
	var keys []shard.Key
	for i := 0; i < 16; i++ {
		k := shard.KeyFromString(strconv.Itoa(i))
		err := dagst.RegisterShard(context.Background(), k, &junkmnt, resCh, RegisterOpts{})
		require.NoError(t, err)
		keys = append(keys, k)
	}

	evts := make([]Trace, 48)
	n, timedOut := sink.Read(evts, 1*time.Second)
	require.False(t, timedOut)
	require.Equal(t, 48, n)

	counts := map[OpType]int{}
	for _, evt := range evts {
		counts[evt.Op]++
	}
	require.Equal(t, counts[OpShardRegister], 16)
	require.Equal(t, counts[OpShardInitialize], 16)
	require.Equal(t, counts[OpShardFail], 16)

	err = dagst.Close()
	require.NoError(t, err)

	t.Run("DoNotRecover", func(t *testing.T) {
		config.RecoverOnStart = DoNotRecover
		dagst, err = NewDAGStore(config)
		require.NoError(t, err)

		err = dagst.Start(context.Background())
		require.NoError(t, err)

		// no events.
		evts := make([]Trace, 16)
		n, timedOut := sink.Read(evts, 1*time.Second)
		require.True(t, timedOut)
		require.Equal(t, 0, n)

		// all shards continue as failed.
		info := dagst.AllShardsInfo()
		require.Len(t, info, 16)
		for _, ss := range info {
			require.Equal(t, ShardStateErrored, ss.ShardState)
			require.Error(t, ss.Error)
		}
	})

	t.Run("RecoverNow", func(t *testing.T) {
		config.RecoverOnStart = RecoverNow
		dagst, err = NewDAGStore(config)
		require.NoError(t, err)

		err = dagst.Start(context.Background())
		require.NoError(t, err)

		// 32 events: recovery and failure.
		evts := make([]Trace, 32)
		n, timedOut := sink.Read(evts, 1*time.Second)
		require.False(t, timedOut)
		require.Equal(t, 32, n)

		counts := map[OpType]int{}
		for _, evt := range evts {
			counts[evt.Op]++
		}
		require.Equal(t, counts[OpShardRecover], 16)
		require.Equal(t, counts[OpShardFail], 16)

		// all shards continue as failed.
		info := dagst.AllShardsInfo()
		require.Len(t, info, 16)
		for _, ss := range info {
			require.Equal(t, ShardStateErrored, ss.ShardState)
			require.Error(t, ss.Error)
		}
	})

	t.Run("RecoverOnAcquire", func(t *testing.T) {
		config.RecoverOnStart = RecoverOnAcquire
		dagst, err = NewDAGStore(config)
		require.NoError(t, err)

		err = dagst.Start(context.Background())
		require.NoError(t, err)

		// 0 events.
		evts := make([]Trace, 32)
		n, timedOut := sink.Read(evts, 500*time.Millisecond)
		require.True(t, timedOut)
		require.Equal(t, 0, n)

		// try to acquire every shard; all fail.
		for _, k := range keys {
			ch := make(chan ShardResult)
			err := dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
			require.NoError(t, err)
			res := <-ch
			require.Equal(t, k, res.Key)
			require.Error(t, res.Error)
			require.Nil(t, res.Accessor)
		}

		// all shards continue as failed.
		info := dagst.AllShardsInfo()
		require.Len(t, info, 16)
		for _, ss := range info {
			require.Equal(t, ShardStateErrored, ss.ShardState)
			require.Error(t, ss.Error)
		}

		// 48 events: acquire, recover, fail.
		evts = make([]Trace, 64)
		n, timedOut = sink.Read(evts, 500*time.Millisecond)
		require.True(t, timedOut)
		require.Equal(t, 48, n)

		counts := map[OpType]int{}
		for _, evt := range evts[:48] {
			counts[evt.Op]++
		}
		require.Equal(t, counts[OpShardAcquire], 16)
		require.Equal(t, counts[OpShardRecover], 16)
		require.Equal(t, counts[OpShardFail], 16)

		// a second acquire for each shard will also fail, and will not trigger
		// any recovery events.
		for _, k := range keys {
			ch := make(chan ShardResult)
			err := dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
			require.NoError(t, err)
			res := <-ch
			require.Equal(t, k, res.Key)
			require.Error(t, res.Error)
			require.Nil(t, res.Accessor)
		}

		// 16 events: acquire, and no more. verify that reading more times out.
		evts = make([]Trace, 32)
		n, timedOut = sink.Read(evts, 500*time.Millisecond)
		require.True(t, timedOut)
		require.Equal(t, 16, n)

		counts = map[OpType]int{}
		for _, evt := range evts[:16] {
			require.Equal(t, OpShardAcquire, evt.Op)
		}
	})

}

// TestFailingAcquireErrorPropagates tests that if multiple acquierers are
// queued, and the fetch fails, all acquires will be notified of the failure.
func TestFailingAcquireErrorPropagates(t *testing.T) {
	r := testRegistry(t)

	err := r.Register("block", newBlockingMount(&mount.FSMount{FS: testdata.FS}))
	require.NoError(t, err)

	dir := t.TempDir()
	sink := tracer(128)
	config := Config{
		MountRegistry: r,
		TransientsDir: dir,
		TraceCh:       sink,
	}
	dagst, err := NewDAGStore(config)
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// create a junk mount and front it with a blocking mount.
	junkmnt := *junkmnt
	mnt := newBlockingMount(&junkmnt)

	// register with lazy init, so that the moun isn't hit until the first acquire.
	k := shard.KeyFromString("foo")
	ch := make(chan ShardResult, 128)
	err = dagst.RegisterShard(context.Background(), k, mnt, ch, RegisterOpts{LazyInitialization: true})
	require.NoError(t, err)
	res := <-ch
	require.NoError(t, res.Error)

	// request five acquires back to back.
	require.NoError(t, dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{}))
	require.NoError(t, dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{}))
	require.NoError(t, dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{}))
	require.NoError(t, dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{}))
	require.NoError(t, dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{}))

	select {
	case <-ch:
		t.FailNow()
	case <-time.After(1 * time.Second):
		// acquires still parked, good.
	}

	// unblock the mount.
	mnt.UnblockNext(1)

	// all acquires will the same error
	for i := 0; i < 5; i++ {
		res := <-ch
		require.Error(t, res.Error)
		require.Contains(t, res.Error.Error(), "invalid header: malformed stream: invalid appearance of bytes token; expected map key")
		require.Nil(t, res.Accessor)
	}
}

func TestTransientReusedOnRestart(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dir := t.TempDir()
	r := testRegistry(t)
	idx := index.NewMemoryRepo()
	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		Datastore:     ds,
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, carv2mnt, ch, RegisterOpts{})
	require.NoError(t, err)
	res := <-ch
	require.NoError(t, res.Error)

	// allow some time for fetching and indexing.
	time.Sleep(1 * time.Second)

	err = dagst.Close()
	require.NoError(t, err)

	dagst, err = NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		Datastore:     ds,
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// make sure the transient is populated, and it exists.
	path := dagst.shards[k].mount.TransientPath()
	require.NotEmpty(t, path)
	_, err = os.Stat(path)
	require.NoError(t, err)

	// acquire the shard.
	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.NoError(t, err)
	res = <-ch
	require.NoError(t, res.Error)
	require.NotNil(t, res.Accessor)

	// ensure that the count has not been incremented (i.e. the origin was not accessed)
	require.Zero(t, dagst.shards[k].mount.TimesFetched())
}

func TestAcquireFailsWhenIndexGone(t *testing.T) {
	ds := datastore.NewMapDatastore()
	dir := t.TempDir()
	r := testRegistry(t)
	idx := index.NewMemoryRepo()
	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: dir,
		Datastore:     ds,
		IndexRepo:     idx,
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, carv2mnt, ch, RegisterOpts{})
	require.NoError(t, err)
	res := <-ch
	require.NoError(t, res.Error)

	// delete all indices from the repo.
	dropped, err := idx.DropFullIndex(k)
	require.True(t, dropped)
	require.NoError(t, err)

	// now try to acquire the shard, it must fail.
	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.NoError(t, err)
	res = <-ch
	require.Error(t, res.Error)
	require.Nil(t, res.Accessor)

	time.Sleep(500 * time.Millisecond)

	// check that the shard is now in failed state.
	info, err := dagst.GetShardInfo(k)
	require.NoError(t, err)
	require.Equal(t, ShardStateErrored, info.ShardState)
	require.Error(t, info.Error)
}

// TestBlockCallback tests that blocking a callback blocks the dispatcher
// but not the event loop.
func TestBlockCallback(t *testing.T) {
	t.Skip("TODO")
}

func TestWaiterContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan ShardResult)
	notifyCh := make(chan struct{})
	w := &waiter{ctx: ctx, outCh: ch, notifyDead: func() { close(notifyCh) }}
	cancel()
	w.deliver(&ShardResult{})
	_, open := <-notifyCh
	require.False(t, open)
}

func TestAcquireContextCancelled(t *testing.T) {
	r := testRegistry(t)
	err := r.Register("block", newBlockingMount(&mount.FSMount{FS: testdata.FS}))
	require.NoError(t, err)

	dagst, err := NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: t.TempDir(),
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	ch := make(chan ShardResult)
	k := shard.KeyFromString("foo")
	block := newBlockingMount(carv2mnt)
	err = dagst.RegisterShard(context.Background(), k, block, ch, RegisterOpts{LazyInitialization: true})
	require.NoError(t, err)
	res := <-ch
	require.NoError(t, res.Error)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // start with a cancelled context
	err = dagst.AcquireShard(ctx, k, ch, AcquireOpts{})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	select {
	case res := <-ch:
		t.Fatalf("expected no ShardResult, got: %+v", res)
	case <-time.After(1 * time.Second):
	}

	ctx, cancel = context.WithCancel(context.Background())
	err = dagst.AcquireShard(ctx, k, ch, AcquireOpts{})
	require.NoError(t, err)
	block.UnblockNext(1)
	cancel() // cancel immediately after unblocking.

	time.Sleep(1 * time.Second)

	select {
	case res := <-ch:
		t.Fatalf("expected no ShardResult, got: %+v", res)
	case <-time.After(1 * time.Second):
	}

	// event loop continues to operate.
	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.NoError(t, err)
	res = <-ch
	require.NoError(t, res.Error)
	require.NotNil(t, res.Accessor)
	err = res.Accessor.Close()
	require.NoError(t, err)

}

// registerShards registers n shards concurrently, using the CARv2 mount.
func registerShards(t *testing.T, dagst *DAGStore, n int, mnt mount.Mount, opts RegisterOpts) (ret []shard.Key) {
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		k := shard.KeyFromString(fmt.Sprintf("shard-%d", i))
		grp.Go(func() error {
			ch := make(chan ShardResult, 1)
			err := dagst.RegisterShard(context.Background(), k, mnt, ch, opts)
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
		if opts.LazyInitialization {
			require.Equal(t, ShardStateNew, ss.ShardState)
			require.NoError(t, ss.Error)
		} else {
			require.Equal(t, ShardStateAvailable, ss.ShardState)
			require.NoError(t, ss.Error)

			istat, err := dagst.indices.StatFullIndex(k)
			require.NoError(t, err)
			require.True(t, istat.Exists)
		}
	}

	return ret
}

// acquireShard acquires the shard known by key `k` concurrently `n` times.
func acquireShard(t *testing.T, dagst *DAGStore, k shard.Key, n int) []*ShardAccessor {
	accessors := make([]*ShardAccessor, n)

	// acquire
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		i := i
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

			state, err := dagst.GetShardInfo(k)
			if err != nil {
				return err
			} else if state.ShardState != ShardStateServing {
				return fmt.Errorf("expected state ShardStateServing; was: %s", state.ShardState)
			}

			if _, err := bs.Get(context.TODO(), testdata.RootCID); err != nil {
				return err
			}

			accessors[i] = res.Accessor
			return nil
		})
	}

	require.NoError(t, grp.Wait())

	// check shard state.
	info, err := dagst.GetShardInfo(k)
	require.NoError(t, err)
	require.Equal(t, ShardStateServing, info.ShardState)
	require.NoError(t, info.Error)
	// refs should be equal to number of acquirers since we've not closed any acquirer/released any shard.
	require.EqualValues(t, n, info.refs)

	return accessors
}

// releaseAll releases all accessors for a given shard.
func releaseAll(t *testing.T, dagst *DAGStore, k shard.Key, accs []*ShardAccessor) {
	grp, _ := errgroup.WithContext(context.Background())
	for _, acc := range accs {
		// close all accessors.
		grp.Go(acc.Close)
	}

	require.NoError(t, grp.Wait())
	require.Eventually(t, func() bool {
		info, err := dagst.GetShardInfo(k)
		return err == nil && info.ShardState == ShardStateAvailable && info.refs == 0
	}, 5*time.Second, 100*time.Millisecond)

}

func testRegistry(t *testing.T) *mount.Registry {
	r := mount.NewRegistry()
	err := r.Register("fs", &mount.FSMount{FS: testdata.FS})
	require.NoError(t, err)
	err = r.Register("counting", new(mount.Counting))
	require.NoError(t, err)
	return r
}

type Tracer chan Trace

func tracer(buf int) Tracer {
	return make(chan Trace, buf)
}

// Read drains as many traces as len(out), at most. It returns how many
// traces were copied into the slice, and updates the internal read
// counter.
func (m Tracer) Read(dst []Trace, timeout time.Duration) (n int, timedOut bool) {
	for i := range dst {
		select {
		case dst[i] = <-m:
		case <-time.After(timeout):
			return i, true
		}
	}
	return len(dst), false
}

// blockingMount is a mount that proxies to another mount, but it blocks by
// default, unless unblock tokens are added via UnblockNext.
type blockingMount struct {
	mount.Mount
	UnblockCh chan struct{} // exported so that it is a templated field for mounts that were restored after a restart.
	ready     bool
}

func newBlockingMount(mnt mount.Mount) *blockingMount {
	return &blockingMount{Mount: mnt, UnblockCh: make(chan struct{})}
}

// UnblockNext allows as many calls to Fetch() as n to proceed.
func (b *blockingMount) UnblockNext(n int) {
	go func() {
		for i := 0; i < n; i++ {
			b.UnblockCh <- struct{}{}
		}
	}()
}

func (b *blockingMount) Fetch(ctx context.Context) (mount.Reader, error) {
	<-b.UnblockCh
	return b.Mount.Fetch(ctx)
}

func (b *blockingMount) Stat(ctx context.Context) (mount.Stat, error) {
	s, err := b.Mount.Stat(ctx)
	if err != nil {
		return s, err
	}
	s.Ready = b.ready
	return s, err
}

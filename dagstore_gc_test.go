package dagstore

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/dagstore/mount"

	"github.com/filecoin-project/dagstore/shard"

	"github.com/filecoin-project/dagstore/gc"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestDagstoreAutomatedGCSimple(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)

	// only has space for two transients
	maxTransientSize := (st.Size * 2) + (st.Size - 1)
	automatedgcCh := make(chan AutomatedGCResult, 1)

	dagst := dagstoreWithGC(t, testRegistry(t), maxTransientSize, 1.0, 0.5, automatedgcCh)
	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// size of dagstore transient directory is zero
	assertTransientsSize(t, dagst, 0)

	// register shard1
	sk1 := shard.KeyFromString("1")
	require.NoError(t, registerShardSync(t, dagst, sk1, carv2mnt, RegisterOpts{}))
	assertTransientsSize(t, dagst, st.Size)

	// register shard 2
	sk2 := shard.KeyFromString("2")
	require.NoError(t, registerShardSync(t, dagst, sk2, carv2mnt, RegisterOpts{}))
	assertTransientsSize(t, dagst, st.Size*2)

	// access shard 2 so it is not evicted during LRU GC.
	sk2Accessor := acquireShard(t, dagst, sk2, 1)[0]

	// register shard 3 -> should evict shard 1 as shard 2 is in state serving.
	sk3 := shard.KeyFromString("3")
	require.NoError(t, registerShardSync(t, dagst, sk3, carv2mnt, RegisterOpts{}))
	assertTransientsSize(t, dagst, st.Size*2)
	assertEvicted(t, sk1, st.Size*2, st.Size, automatedgcCh)

	// registr shard 4 -> shard 3 gets evicted as shard 2 is in state serving.
	sk4 := shard.KeyFromString("4")
	require.NoError(t, registerShardSync(t, dagst, sk4, carv2mnt, RegisterOpts{}))
	assertTransientsSize(t, dagst, st.Size*2)
	assertEvicted(t, sk3, st.Size*2, st.Size, automatedgcCh)

	// access shard 4
	acquireShard(t, dagst, sk4, 1)

	// register shard 5 -> fails because no shard can be released as both shard 2 and shard 4 are in state serving.
	sk5 := shard.KeyFromString("5")
	err = registerShardSync(t, dagst, sk5, carv2mnt, registerOptsForBackoffRetry())
	require.Contains(t, err.Error(), "not enough space")
	assertTransientsSize(t, dagst, st.Size*2)

	// release shard 2 and wait for it to be available
	require.NoError(t, sk2Accessor.Close())

	require.Eventually(t, func() bool {
		si, err := dagst.GetShardInfo(sk2)
		return err == nil && si.ShardState == ShardStateAvailable
	}, 1*time.Second, 100*time.Millisecond)

	// recover shard 5 -> works and shard 2 is evicted
	require.NoError(t, recoverShard(t, ctx, dagst, sk5))
	assertTransientsSize(t, dagst, st.Size*2)
	assertEvicted(t, sk2, st.Size*2, st.Size, automatedgcCh)
}

func TestDagstoreAutomatedGCConcurrent(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)

	// only has space for 10 transients.
	maxTransientSize := (st.Size * 10) + (st.Size - 1)

	sink := automatedGCTracer(128)

	dagst := dagstoreWithGC(t, testRegistry(t), maxTransientSize, 1.0, 0.5, sink)
	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// size of dagstore transient directory is zero
	assertTransientsSize(t, dagst, 0)

	// register 10 shards
	keys := registerShards(t, dagst, 10, carv2mnt, RegisterOpts{})

	// acquire 2 shards -> so these will never be evicted
	acquireShard(t, dagst, keys[0], 1)
	acquireShard(t, dagst, keys[1], 1)

	// register 10 more shards -> works
	keyset2 := registerShardsWithKeyPrefix(t, dagst, 10, carv2mnt, "test-", 20, RegisterOpts{})

	// assert exactly 10 shards were evicted
	evicted := assertExactlyNEvictedAndMaxDirSize(t, sink, 10, maxTransientSize)

	// In the end, shards [2:10] in the first set should be evicted
	assertEvictedContainsAll(t, evicted, keys[2:])

	// and any two shards in the second shard should be evicted
	assertEvictedContainsAny(t, evicted, keyset2, 2)

	assertTransientsSize(t, dagst, 10*st.Size)
}

func TestDagstoreAutomatedGCConcurrentRegisterAboveSize(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)

	run := func(t *testing.T, n int, maxTransientSize int64) {
		sink := automatedGCTracer(10000)
		dagst := dagstoreWithGC(t, testRegistry(t), maxTransientSize, 1.0, 0.5, sink)

		err = dagst.Start(context.Background())
		require.NoError(t, err)

		registerShards(t, dagst, n, carv2mnt, RegisterOpts{})

		// assert transient dir size never goes up above maximum size
		assertDirSizeConstraint(t, sink, maxTransientSize)
	}

	// don't want max transient dir size to fall at shard size boundaries.

	t.Run("4", func(t *testing.T) {
		run(t, 4, 2*(st.Size-1))
	})

	// don't want max transient dir size to fall at shard size boundaries.
	t.Run("12", func(t *testing.T) {
		run(t, 12, 2*(st.Size+50))
	})

	t.Run("16", func(t *testing.T) {
		run(t, 16, 4*(st.Size+50))
	})

	t.Run("32", func(t *testing.T) {
		run(t, 32, 5*(st.Size-79))
	})

	t.Run("40", func(t *testing.T) {
		run(t, 32, 5*(st.Size-79))
	})
}

type zeroSizedMount struct {
	mount.Mount
}

func (m *zeroSizedMount) Stat(ctx context.Context) (mount.Stat, error) {
	st, err := m.Mount.Stat(ctx)
	st.Size = 0
	return st, err
}

func TestAutomatedGCRestartAndShardSizeMemoization(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)

	// only has space for 10 transients.
	maxTransientSize := (st.Size * 10) + (st.Size - 1)

	r := testRegistry(t)
	err = r.Register("zero", &zeroSizedMount{carv2mnt})
	require.NoError(t, err)

	sink := automatedGCTracer(128)
	dagst := dagstoreWithGC(t, r, maxTransientSize, 1.0, 0.5, sink)
	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 10 shards so dagstore dir becomes full
	keys := registerShards(t, dagst, 10, &zeroSizedMount{carv2mnt}, RegisterOpts{})
	require.Len(t, keys, 10)

	// track which shards were evicted
	evicted := make([]AutomatedGCResult, 1000)
	nEvicted, timeout := sink.Read(evicted, 3*time.Second)
	require.True(t, timeout)
	evicted = evicted[:nEvicted]
	var evictedKeys []shard.Key
	for _, v := range evicted {
		evictedKeys = append(evictedKeys, v.ReclaimedShard)
	}

	// we know the size even after a restart
	sink2 := automatedGCTracer(128)
	trace := tracer(128)
	// restart with space for only one shard but watermark=0.5
	dagst2, err := NewDAGStore(Config{
		Datastore:          dagst.store,
		IndexRepo:          dagst.indices,
		MountRegistry:      r,
		TransientsDir:      dagst.config.TransientsDir,
		TraceCh:            trace,
		AutomatedGCEnabled: true,
		AutomatedGCConfig: &AutomatedGCConfig{
			DefaultReservationSize:    st.Size / 4,
			MaxTransientDirSize:       st.Size,
			TransientsGCWatermarkHigh: 1.0,
			TransientsGCWatermarkLow:  0.5,
			GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
			AutomatedGCTraceCh:        sink2,
		},
	})
	require.NoError(t, err)
	dagst2.store = dagst.store

	require.NoError(t, dagst2.Start(context.Background()))

	for _, k := range keys {
		si, err := dagst2.GetShardInfo(k)
		require.NoError(t, err)
		require.EqualValues(t, st.Size, si.TransientSize)
	}

	// assert remaining shards were reclaimed this time.
	evicted2 := assertExactlyNEvicted(t, sink2, len(keys)-nEvicted)
	assertEvictedDoesNotContain(t, evicted2, evictedKeys)

	assertTransientsSize(t, dagst2, 0)

	// try downloading the transient now -> there should be only one reservation of the known size and no eviction.
	acquireShard(t, dagst2, keys[0], 1)
	assertNoEviction(t, sink2)

	traces := make([]Trace, 2)
	n, timedOut := trace.Read(traces, 3*time.Second)
	require.Equal(t, 2, n)
	require.False(t, timedOut)
	require.EqualValues(t, OpShardAcquire, traces[0].Op)
	require.EqualValues(t, OpShardReserveTransient, traces[1].Op)
	require.EqualValues(t, st.Size, traces[1].TransientDirSizeCounter)

	// try registering a shard -> fails as space already taken up by acquired shard
	err = registerShardSync(t, dagst2, shard.KeyFromString("test"), carv2mnt, registerOptsForBackoffRetry())
	require.Contains(t, err.Error(), "not enough space")
	assertTransientsSize(t, dagst, st.Size)
}

type AutomatedGCTracer chan AutomatedGCResult

func automatedGCTracer(buf int) AutomatedGCTracer {
	return make(chan AutomatedGCResult, buf)
}

// Read drains as many traces as len(dst), at most. It returns how many
// traces were copied into the slice, and updates the internal read
// counter.
func (m AutomatedGCTracer) Read(dst []AutomatedGCResult, timeout time.Duration) (n int, timedOut bool) {
	for i := range dst {
		select {
		case dst[i] = <-m:
		case <-time.After(timeout):
			return i, true
		}
	}
	return len(dst), false
}

func assertNoEviction(t *testing.T, sink AutomatedGCTracer) {
	evicted := make([]AutomatedGCResult, 1)
	n, timeout := sink.Read(evicted, 1*time.Second)
	require.Zero(t, n)
	require.True(t, timeout)
}

func assertExactlyNEvictedAndMaxDirSize(t *testing.T, sink AutomatedGCTracer, N int, maxTransientDirSize int64) []AutomatedGCResult {
	evicted := assertExactlyNEvicted(t, sink, N)

	// ensure we never exceed the max transient dir size
	for _, v := range evicted {
		require.True(t, v.TransientsDirSizeAfterReclaim < maxTransientDirSize)
		require.True(t, v.TransientsDirSizeBeforeReclaim < maxTransientDirSize)
		require.True(t, v.TransientsAccountingAfterReclaim < maxTransientDirSize)
		require.True(t, v.TransientsAccountingBeforeReclaim < maxTransientDirSize)
	}

	return evicted
}

func assertExactlyNEvicted(t *testing.T, sink AutomatedGCTracer, N int) []AutomatedGCResult {
	evicted := make([]AutomatedGCResult, N)
	n, timeout := sink.Read(evicted, 3*time.Second)
	require.Equal(t, N, n)
	require.False(t, timeout)

	// no more values in the sink
	n, timeout = sink.Read(make([]AutomatedGCResult, N), 1*time.Second)
	require.Zero(t, n)
	require.True(t, timeout)

	return evicted
}

func assertDirSizeConstraint(t *testing.T, sink AutomatedGCTracer, maxTransientDirSize int64) {
	evicted := make([]AutomatedGCResult, 10000)
	n, timeout := sink.Read(evicted, 3*time.Second)
	require.NotZero(t, n)
	require.True(t, timeout)

	// ensure we never exceed the max transient dir size
	for _, v := range evicted {
		require.True(t, v.TransientsDirSizeAfterReclaim < maxTransientDirSize)
		require.True(t, v.TransientsDirSizeBeforeReclaim < maxTransientDirSize)
		require.True(t, v.TransientsAccountingAfterReclaim < maxTransientDirSize)
		require.True(t, v.TransientsAccountingBeforeReclaim < maxTransientDirSize)
	}
}

func assertEvictedDoesNotContain(t *testing.T, evicted []AutomatedGCResult, notContain []shard.Key) {
	evmap := make(map[shard.Key]struct{})
	for _, k := range evicted {
		evmap[k.ReclaimedShard] = struct{}{}
	}
	require.Len(t, evmap, 10)

	for _, k := range notContain {
		_, ok := evmap[k]
		require.False(t, ok)
	}
}

func assertEvictedContainsAll(t *testing.T, evicted []AutomatedGCResult, contains []shard.Key) {
	evmap := make(map[shard.Key]struct{})
	for _, k := range evicted {
		evmap[k.ReclaimedShard] = struct{}{}
	}
	require.Len(t, evmap, 10)

	for _, k := range contains {
		_, ok := evmap[k]
		require.True(t, ok)
	}
}

func assertEvictedContainsAny(t *testing.T, evicted []AutomatedGCResult, contains []shard.Key, n int) {
	evmap := make(map[shard.Key]struct{})
	for _, k := range evicted {
		evmap[k.ReclaimedShard] = struct{}{}
	}
	require.Len(t, evmap, 10)

	count := 0
	for _, k := range contains {
		_, ok := evmap[k]
		if ok {
			count++
		}
	}
	require.Equal(t, n, count)
}

func dagstoreWithGC(t *testing.T, r *mount.Registry, maxTransientSize int64,
	highWaterMark float64, lowWaterMark float64, automatedgcCh chan AutomatedGCResult) *DAGStore {
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	dagst, err := NewDAGStore(Config{
		MaxConcurrentReadyFetches: 5,
		MountRegistry:             r,
		TransientsDir:             t.TempDir(),
		Datastore:                 store,
		AutomatedGCEnabled:        true,
		AutomatedGCConfig: &AutomatedGCConfig{
			MaxTransientDirSize:       maxTransientSize,
			TransientsGCWatermarkHigh: highWaterMark,
			TransientsGCWatermarkLow:  lowWaterMark,
			GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
			AutomatedGCTraceCh:        automatedgcCh,
		},
	})
	require.NoError(t, err)

	return dagst
}

func assertTransientsSize(t *testing.T, dagst *DAGStore, expected int64) {
	sz, err := dagst.transientDirSize()
	require.NoError(t, err)
	require.EqualValues(t, expected, sz)
}

func registerOptsForBackoffRetry() RegisterOpts {
	return RegisterOpts{
		ReservationOpts: []mount.ReservationGatedDownloaderOpt{
			mount.ReservationBackOffRetryOpt(100*time.Millisecond, 300*time.Millisecond, 2, 3),
		},
	}
}

func recoverShard(t *testing.T, ctx context.Context, dagst *DAGStore, sk shard.Key) error {
	ch := make(chan ShardResult)
	require.NoError(t, dagst.RecoverShard(ctx, sk, ch, RecoverOpts{}))
	res := <-ch
	return res.Error
}

func assertEvicted(t *testing.T, key shard.Key, before int64, after int64, ch chan AutomatedGCResult) {
	evicted := <-ch
	require.EqualValues(t, key, evicted.ReclaimedShard)
	require.EqualValues(t, before, evicted.TransientsDirSizeBeforeReclaim)
	require.EqualValues(t, after, evicted.TransientsDirSizeAfterReclaim)
}

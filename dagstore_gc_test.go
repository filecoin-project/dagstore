package dagstore

import (
	"context"
	"errors"
	"fmt"
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
	maxTransientSize := (st.Size * 2) + 20
	automatedgcCh := make(chan *AutomatedGCResult, 1)

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	dagst, err := NewDAGStore(Config{
		MountRegistry:      testRegistry(t),
		TransientsDir:      t.TempDir(),
		Datastore:          store,
		AutomatedGCEnabled: true,
		AutomatedGCConfig: &AutomatedGCConfig{
			MaxTransientDirSize:       maxTransientSize,
			TransientsGCWatermarkHigh: 1.0,
			TransientsGCWatermarkLow:  0.5,
			GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
			AutomatedGCTraceCh:        automatedgcCh,
		},
	})
	require.NoError(t, err)

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
	sa2 := acquireShard(t, dagst, sk2, 1)[0]

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
	require.Contains(t, registerShardSync(t, dagst, sk5, carv2mnt, RegisterOpts{
		ReservationOpts: []mount.ReservationGatedDownloaderOpt{
			mount.ReservationBackOffRetryOpt(100*time.Millisecond, 300*time.Millisecond, 2, 3),
		},
	}).Error(), "not enough space")
	assertTransientsSize(t, dagst, st.Size*2)

	// release shard 2
	require.NoError(t, sa2.Close())

	require.Eventually(t, func() bool {
		si, err := dagst.GetShardInfo(sk2)
		return err == nil && si.ShardState == ShardStateAvailable
	}, 1*time.Second, 100*time.Millisecond)

	// recover shard 5 -> works and shard 2 is evicted
	ch := make(chan ShardResult)
	require.NoError(t, dagst.RecoverShard(ctx, sk5, ch, RecoverOpts{}))
	res := <-ch
	require.NoError(t, res.Error)
	assertTransientsSize(t, dagst, st.Size*2)
	assertEvicted(t, sk2, st.Size*2, st.Size, automatedgcCh)
}

func TestDagstoreAutomatedGCConcurrent(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)

	// only has space for 10 transients.
	maxTransientSize := (st.Size * 10) + 20
	automatedgcCh := make(chan *AutomatedGCResult, 1)

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	dagst, err := NewDAGStore(Config{
		MountRegistry:      testRegistry(t),
		TransientsDir:      t.TempDir(),
		Datastore:          store,
		AutomatedGCEnabled: true,
		AutomatedGCConfig: &AutomatedGCConfig{
			MaxTransientDirSize:       maxTransientSize,
			TransientsGCWatermarkHigh: 1.0,
			TransientsGCWatermarkLow:  0.5,
			GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
			AutomatedGCTraceCh:        automatedgcCh,
		},
	})
	require.NoError(t, err)

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// size of dagstore transient directory is zero
	assertTransientsSize(t, dagst, 0)

	// register 10 shards
	keys := registerShards(t, dagst, 10, carv2mnt, RegisterOpts{})

	// acquire 2
	acquireShard(t, dagst, keys[0], 1)
	acquireShard(t, dagst, keys[1], 1)

	evicted := make(chan *AutomatedGCResult, 8)

	donech := make(chan struct{})
	go func() {
		defer func() { close(evicted) }()
		for {
			select {
			case v := <-automatedgcCh:
				evicted <- v
			case <-donech:
				return
			}
		}
	}()

	var acqs []*ShardAccessor

	// register 10 shards -> works
	for i := 10; i < 18; i++ {
		key := shard.KeyFromString(fmt.Sprintf("%d", i))
		require.NoError(t, registerShardSync(t, dagst, key, carv2mnt, RegisterOpts{}))
		sa := acquireShard(t, dagst, key, 1)
		acqs = append(acqs, sa[0])
	}
	donech <- struct{}{}

	// In the end, shards [2:10] in the first set should be evicted
	evmap := make(map[shard.Key]struct{})
	for k := range evicted {
		evmap[k.ReclaimedShard] = struct{}{}
	}
	require.Len(t, evmap, 8)
	for i := 2; i < 10; i++ {
		_, ok := evmap[keys[i]]
		require.True(t, ok)
	}

	sz, err := dagst.transientDirSize()
	require.NoError(t, err)
	require.EqualValues(t, 10*st.Size, sz)
}

func TestDagstoreAutomatedGCConcurrentRegisterAboveSize(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)

	run := func(t *testing.T, n int, maxTransientSize int64) {
		automatedgcCh := make(chan *AutomatedGCResult)

		store := dssync.MutexWrap(datastore.NewMapDatastore())
		dagst, err := NewDAGStore(Config{
			MountRegistry:      testRegistry(t),
			TransientsDir:      t.TempDir(),
			Datastore:          store,
			AutomatedGCEnabled: true,
			AutomatedGCConfig: &AutomatedGCConfig{
				MaxTransientDirSize:       maxTransientSize,
				TransientsGCWatermarkHigh: 1.0,
				TransientsGCWatermarkLow:  0.5,
				GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
				AutomatedGCTraceCh:        automatedgcCh,
			},
		})
		require.NoError(t, err)

		// ensure transient dir size never goes up above maximum size
		doneCh := make(chan struct{}, 1)
		errorCh := make(chan error, 1)
		go func() {
			for {
				select {
				case val := <-automatedgcCh:
					if val.TransientsDirSizeBeforeReclaim > maxTransientSize ||
						val.TransientsDirSizeAfterReclaim > maxTransientSize || val.TransientsAccountingBeforeReclaim > maxTransientSize ||
						val.TransientsAccountingAfterReclaim > maxTransientSize {
						errorCh <- errors.New("transient dir size went above maximum allowed")
						return
					}
				case <-doneCh:
					close(errorCh)
					return
				}
			}
		}()

		err = dagst.Start(context.Background())
		require.NoError(t, err)

		registerShards(t, dagst, n, carv2mnt, RegisterOpts{})
		close(doneCh)
		require.NoError(t, <-errorCh)
	}

	// don't want max transient dir size to fall at shard size boundaries.

	t.Run("4", func(t *testing.T) {
		run(t, 4, 2*(st.Size-100))
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

type ZeroSizeMount struct {
	mount.Mount
}

func (m *ZeroSizeMount) Stat(ctx context.Context) (mount.Stat, error) {
	st, err := m.Mount.Stat(ctx)
	st.Size = 0
	return st, err
}

func TestShardSizeIsMemoized(t *testing.T) {
	ctx := context.Background()
	st, err := carv2mnt.Stat(ctx)
	require.NoError(t, err)
	fmt.Println("\n Mount size is", st.Size)

	// only has space for 10 transients.
	maxTransientSize := (st.Size * 10) + 20

	automatedgcCh := make(chan *AutomatedGCResult, 1)

	r := testRegistry(t)
	err = r.Register("zero", &ZeroSizeMount{carv2mnt})
	require.NoError(t, err)

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	dagst, err := NewDAGStore(Config{
		MaxConcurrentReadyFetches: 5,
		MountRegistry:             r,
		TransientsDir:             t.TempDir(),
		Datastore:                 store,
		AutomatedGCEnabled:        true,
		AutomatedGCConfig: &AutomatedGCConfig{
			DefaultReservationSize:    st.Size / 3,
			MaxTransientDirSize:       maxTransientSize,
			TransientsGCWatermarkHigh: 1.0,
			TransientsGCWatermarkLow:  0.5,
			GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
			AutomatedGCTraceCh:        automatedgcCh,
		},
	})
	require.NoError(t, err)

	doneCh := make(chan struct{}, 1)
	errorCh := make(chan error, 1)
	go func() {
		for {
			select {
			case val := <-automatedgcCh:
				if val.TransientsDirSizeBeforeReclaim > maxTransientSize ||
					val.TransientsDirSizeAfterReclaim > maxTransientSize ||
					val.TransientsAccountingBeforeReclaim > maxTransientSize ||
					val.TransientsAccountingAfterReclaim > maxTransientSize {
					errorCh <- errors.New("transient dir size went above maximum allowed")
					return
				}
			case <-doneCh:
				close(errorCh)
				return
			}
		}
	}()

	err = dagst.Start(context.Background())
	require.NoError(t, err)

	// register 10 shards so dagstore dir becomes full
	keys := registerShards(t, dagst, 10, &ZeroSizeMount{carv2mnt}, RegisterOpts{})
	require.Len(t, keys, 10)
	close(doneCh)
	require.NoError(t, <-errorCh)

	// we know the size even after a restart
	dagst, err = NewDAGStore(Config{
		MountRegistry: r,
		TransientsDir: t.TempDir(),
		Datastore:     store,
	})
	require.NoError(t, err)
	require.NoError(t, dagst.Start(context.Background()))

	for _, k := range keys {
		si, err := dagst.GetShardInfo(k)
		require.NoError(t, err)
		require.EqualValues(t, st.Size, si.TransientSize)
	}
}

func assertEvicted(t *testing.T, key shard.Key, before int64, after int64, ch chan *AutomatedGCResult) {
	evicted := <-ch
	require.EqualValues(t, key, evicted.ReclaimedShard)
	require.EqualValues(t, before, evicted.TransientsDirSizeBeforeReclaim)
	require.EqualValues(t, after, evicted.TransientsDirSizeAfterReclaim)
}

func assertTransientsSize(t *testing.T, dagst *DAGStore, expected int64) {
	sz, err := dagst.transientDirSize()
	require.NoError(t, err)
	require.EqualValues(t, expected, sz)
}

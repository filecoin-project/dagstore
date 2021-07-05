package dagstore

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"testing"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	//go:embed testdata/sample-v1.car
	carv1 []byte
	//go:embed testdata/sample-wrapped-v2.car
	carv2 []byte

	// rootCID is the root CID of the carv2 for testing.
	rootCID cid.Cid
)

func init() {
	_ = logging.SetLogLevel("dagstore", "DEBUG")

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
	dir := t.TempDir()
	dagst, err := NewDAGStore(Config{
		ScratchSpaceDir: dir,
		Datastore:       datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan Result, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.BytesMount{Bytes: carv1}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.Error(t, res.Error)
	require.Contains(t, res.Error.Error(), "invalid car version")
	require.EqualValues(t, k, res.Key)
	require.Nil(t, res.Accessor)
}

func TestRegisterCarV2(t *testing.T) {
	dir := t.TempDir()
	dagst, err := NewDAGStore(Config{
		ScratchSpaceDir: dir,
		Datastore:       datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan Result, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.BytesMount{Bytes: carv2}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)
	require.EqualValues(t, k, res.Key)
	require.Nil(t, res.Accessor)
}

func TestRegisterConcurrentShards(t *testing.T) {
	run := func(t *testing.T, n int) {
		dir := t.TempDir()
		dagst, err := NewDAGStore(Config{
			ScratchSpaceDir: dir,
			Datastore:       datastore.NewMapDatastore(),
		})
		require.NoError(t, err)

		grp, _ := errgroup.WithContext(context.Background())
		for i := 0; i < n; i++ {
			i := i
			grp.Go(func() error {
				ch := make(chan Result, 1)
				k := shard.KeyFromString(fmt.Sprintf("shard-%d", i))
				err := dagst.RegisterShard(context.Background(), k, &mount.BytesMount{Bytes: carv2}, ch, RegisterOpts{})
				if err != nil {
					return err
				}
				res := <-ch
				return res.Error
			})
		}
		require.NoError(t, grp.Wait())
	}

	t.Run("16", func(t *testing.T) { run(t, 16) })
	t.Run("32", func(t *testing.T) { run(t, 32) })
	t.Run("64", func(t *testing.T) { run(t, 64) })
	t.Run("128", func(t *testing.T) { run(t, 128) })
	t.Run("256", func(t *testing.T) { run(t, 256) })
}

func TestAcquireInexistentShard(t *testing.T) {
	dir := t.TempDir()
	dagst, err := NewDAGStore(Config{
		ScratchSpaceDir: dir,
		Datastore:       datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan Result, 1)
	k := shard.KeyFromString("foo")
	err = dagst.AcquireShard(context.Background(), k, ch, AcquireOpts{})
	require.Error(t, err)
}

func TestAcquireAfterRegisterWait(t *testing.T) {
	t.Skip("uncomment when https://github.com/ipfs/go-cid/issues/126#issuecomment-872364155 is fixed")

	dir := t.TempDir()
	dagst, err := NewDAGStore(Config{
		ScratchSpaceDir: dir,
		Datastore:       datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan Result, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.BytesMount{Bytes: carv2}, ch, RegisterOpts{})
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
	dir := t.TempDir()
	dagst, err := NewDAGStore(Config{
		ScratchSpaceDir: dir,
		Datastore:       datastore.NewMapDatastore(),
	})
	require.NoError(t, err)

	ch := make(chan Result, 1)
	k := shard.KeyFromString("foo")
	err = dagst.RegisterShard(context.Background(), k, &mount.BytesMount{Bytes: carv2}, ch, RegisterOpts{})
	require.NoError(t, err)

	res := <-ch
	require.NoError(t, res.Error)

	run := func(t *testing.T, n int) {
		grp, _ := errgroup.WithContext(context.Background())
		for i := 0; i < n; i++ {
			grp.Go(func() error {
				ch := make(chan Result, 1)
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

// TestBlockCallback tests that blocking a callback blocks the dispatcher
// but not the event loop.
func TestBlockCallback(t *testing.T) {
	t.Skip("TODO")
}

package dagstore

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	measure "github.com/ipfs/go-ds-measure"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/rand"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/dagstore/testdata"
)

func BenchmarkDagstore(b *testing.B) {
	transientsDir, err := ioutil.TempDir("", "markets-repo-dir-transients")
	if err != nil {
		panic(err)
	}

	dsDir, err := ioutil.TempDir("", "markets-ds-dir")
	if err != nil {
		panic(err)
	}

	indexDir, err := ioutil.TempDir("", "markets-index-dir")
	if err != nil {
		panic(err)
	}

	ds, err := newDatastore(dsDir)
	if err != nil {
		panic(err)
	}

	r := mount.NewRegistry()
	err = r.Register("fs", &mount.FSMount{FS: testdata.FS})
	if err != nil {
		panic(err)
	}
	err = r.Register("counting", new(mount.Counting))
	if err != nil {
		panic(err)
	}

	irepo, err := index.NewFSRepo(indexDir)
	if err != nil {
		panic(err)
	}

	dagst, err := NewDAGStore(Config{
		IndexRepo:     irepo,
		MountRegistry: r,
		TransientsDir: transientsDir,
		Datastore:     ds,
	})
	if err != nil {
		panic(err)
	}

	err = dagst.Start(context.Background())
	if err != nil {
		panic(err)
	}

	for n := 0; n < b.N; n++ {
		ch := make(chan ShardResult, 1)
		k := shard.KeyFromString(rand.String(10))

		// even though the fs mount has an empty path, the existing transient will get us through registration.
		err = dagst.RegisterShard(context.Background(), k, &mount.FSMount{FS: testdata.FS, Path: ""}, ch, RegisterOpts{ExistingTransient: testdata.RootPathCarV2})
		if err != nil {
			panic(err)
		}

		_ = <-ch
	}
}

func newDatastore(dir string) (ds.Batching, error) {
	// Create the datastore directory if it doesn't exist yet.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, xerrors.Errorf("failed to create directory %s for DAG store datastore: %w", dir, err)
	}

	// Create a new LevelDB datastore
	dstore, err := levelds.NewDatastore(dir, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to open datastore for DAG store: %w", err)
	}
	// Keep statistics about the datastore
	mds := measure.New("measure.", dstore)
	return mds, nil
}

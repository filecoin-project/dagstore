package mount

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

//go:embed testdata/*
var testdata embed.FS

const (
	carv1path = "testdata/sample-v1.car"
	carv2path = "testdata/sample-wrapped-v2.car"
)

func TestUpgrade(t *testing.T) {
	tcs := map[string]struct {
		setup     func(t *testing.T, key shard.Key, rootDir string)
		createMnt func(t *testing.T, key shard.Key, rootDir string) Mount
		initial   string

		// expectations
		verify                  func(t *testing.T, u *Upgrader, key shard.Key, rootDir string)
		expectedContentFilePath string
	}{
		"no transient file when underlying mount has all capabilities": {
			createMnt: func(t *testing.T, key shard.Key, rootDir string) Mount {
				return &FileMount{carv1path}
			},
			verify: func(t *testing.T, u *Upgrader, key shard.Key, rootDir string) {
				fs, err := ioutil.ReadDir(rootDir)
				require.NoError(t, err)
				require.Empty(t, fs)
				_, err = os.Stat(u.transientFilePath())
				require.Error(t, err)
			},
			expectedContentFilePath: carv1path,
		},
		"no transient file is created when transient file already exists": {
			setup: func(t *testing.T, key shard.Key, rootDir string) {
				f, err := os.Create(filepath.Join(rootDir, key.String()))
				require.NoError(t, err)

				f2, err := os.Open(carv1path)
				require.NoError(t, err)
				_, err = io.Copy(f, f2)
				require.NoError(t, err)
				require.NoError(t, f2.Close())

				require.NoError(t, f.Close())
			},
			initial: "should never be used",

			createMnt: func(t *testing.T, key shard.Key, rootDir string) Mount {
				return &FSMount{testdata, carv2path}
			},

			verify: func(t *testing.T, u *Upgrader, key shard.Key, rootDir string) {
				_, err := os.Stat(u.transientFilePath())
				require.NoError(t, err)
			},
			expectedContentFilePath: carv1path,
		},

		"no transient file is created if there is no initial copy given by the client": {
			createMnt: func(t *testing.T, key shard.Key, rootDir string) Mount {
				return &FSMount{testdata, carv2path}
			},

			verify: func(t *testing.T, u *Upgrader, key shard.Key, rootDir string) {
				fs, err := ioutil.ReadDir(rootDir)
				require.NoError(t, err)
				require.Empty(t, fs)
				_, err = os.Stat(u.transientFilePath())
				require.Error(t, err)
			},
			expectedContentFilePath: carv2path,
		},

		"transient file is copied from user's initial file": {
			initial: carv1path,

			createMnt: func(t *testing.T, key shard.Key, rootDir string) Mount {
				return &FSMount{testdata, carv2path} // purposely giving a different file here.
			},

			verify: func(t *testing.T, u *Upgrader, key shard.Key, rootDir string) {
				_, err := os.Stat(u.transientFilePath())
				require.NoError(t, err)

				// read the contents of the transient file.
				tf, err := os.Open(u.transientFilePath())
				require.NoError(t, err)
				defer tf.Close()
				bz, err := ioutil.ReadAll(tf)
				require.NoError(t, err)
				require.NoError(t, tf.Close())

				// read the contents of the initial file -> they should match.
				f, err := os.Open(carv1path)
				require.NoError(t, err)
				defer f.Close()
				bz2, err := ioutil.ReadAll(f)
				require.NoError(t, err)
				require.NoError(t, f.Close())
				require.EqualValues(t, bz, bz2)
			},
			expectedContentFilePath: carv1path,
		},
	}

	ctx := context.Background()

	for name, tc := range tcs {
		tcc := tc
		t.Run(name, func(t *testing.T) {
			key := shard.KeyFromString(fmt.Sprintf("%d", rand.Uint64()))
			rootDir := t.TempDir()
			if tcc.setup != nil {
				tcc.setup(t, key, rootDir)
			}

			mnt := tcc.createMnt(t, key, rootDir)

			u, err := Upgrade(mnt, rootDir, key, tcc.initial)
			require.NoError(t, err)
			require.NotNil(t, u)
			tcc.verify(t, u, key, rootDir)

			// fetch and verify contents
			rd, err := u.Fetch(ctx)
			require.NoError(t, err)
			require.NotNil(t, rd)
			bz, err := ioutil.ReadAll(rd)
			require.NoError(t, err)
			require.NotEmpty(t, bz)
			require.NoError(t, rd.Close())

			f, err := os.Open(tcc.expectedContentFilePath)
			require.NoError(t, err)
			bz2, err := ioutil.ReadAll(f)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			require.EqualValues(t, bz2, bz)

			// no transient file should exist as it's cleaned up when reader is closed and ref count drops to zero.
			_, err = os.Stat(u.transientFilePath())
			require.Error(t, err)

		})
	}
}

type countingMount struct {
	mu sync.Mutex
	n  int

	Mount
}

func (c *countingMount) Fetch(ctx context.Context) (Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.n++

	return c.Mount.Fetch(ctx)
}

func (c *countingMount) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func TestUpgraderDeduplicatesRemote(t *testing.T) {
	ctx := context.Background()
	mnt := &countingMount{Mount: &FSMount{testdata, carv2path}}

	key := shard.KeyFromString(fmt.Sprintf("%d", rand.Uint64()))
	rootDir := t.TempDir()
	u, err := Upgrade(mnt, rootDir, key, "")
	require.NoError(t, err)
	require.Zero(t, mnt.Count())

	// now fetch in parallel
	cnt := 20
	var mu sync.Mutex
	readers := make([]Reader, 0, cnt)
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < cnt; i++ {
		grp.Go(func() error {
			rd, err := u.Fetch(ctx)
			if err != nil {
				return err
			}
			mu.Lock()
			readers = append(readers, rd)
			mu.Unlock()
			return nil
		})
	}
	require.NoError(t, grp.Wait())
	// file should have been fetched only once
	require.EqualValues(t, 1, mnt.Count())
	// ensure transient exists
	_, err = os.Stat(u.transientFilePath())
	require.NoError(t, err)

	// ensure all readers have the correct content and close them all.
	mu.Lock()
	defer mu.Unlock()

	carF, err := os.Open(carv2path)
	require.NoError(t, err)
	carBytes, err := ioutil.ReadAll(carF)
	require.NoError(t, err)
	require.NoError(t, carF.Close())

	grp2, _ := errgroup.WithContext(context.Background())
	for _, rd := range readers {
		rdc := rd
		grp2.Go(func() error {
			bz, err := ioutil.ReadAll(rdc)
			if err != nil {
				return err
			}
			if err := rdc.Close(); err != nil {
				return err
			}

			if !bytes.Equal(carBytes, bz) {
				return errors.New("contents do not match")
			}
			return nil
		})
	}
	require.NoError(t, grp2.Wait())

	// file should have been fetched only once
	require.EqualValues(t, 1, mnt.Count())
	// ensure transient has been removed as all readers have been closed.
	_, err = os.Stat(u.transientFilePath())
	require.Error(t, err)

	// fetch again and file should have been fetched twice
	rd, err := u.Fetch(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, mnt.Count())
	_, err = os.Stat(u.transientFilePath())
	require.NoError(t, err)

	require.NoError(t, rd.Close())
	_, err = os.Stat(u.transientFilePath())
	require.Error(t, err)
}

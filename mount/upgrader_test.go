package mount

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/filecoin-project/dagstore/testdata"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestUpgrade(t *testing.T) {
	tcs := map[string]struct {
		setup     func(t *testing.T, key string, rootDir string)
		createMnt func(t *testing.T, key string, rootDir string) Mount
		initial   string

		// expectations
		verify                  func(t *testing.T, u *Upgrader, key string, rootDir string)
		expectedContentFilePath string
	}{
		"no transient file when underlying mount has all capabilities": {
			createMnt: func(t *testing.T, key string, rootDir string) Mount {
				return &FileMount{"../" + testdata.RootPathCarV1}
			},
			verify: func(t *testing.T, u *Upgrader, key string, rootDir string) {
				fs, err := ioutil.ReadDir(rootDir)
				require.NoError(t, err)
				require.Empty(t, fs)
				_, err = os.Stat(u.TransientPath())
				require.Error(t, err)
			},
			expectedContentFilePath: "../" + testdata.RootPathCarV1,
		},

		"transient file is copied from user's initial file": {
			initial: "../" + testdata.RootPathCarV1,

			createMnt: func(t *testing.T, key string, rootDir string) Mount {
				return &FSMount{testdata.FS, testdata.FSPathCarV2} // purposely giving a different file here.
			},

			verify: func(t *testing.T, u *Upgrader, key string, rootDir string) {
				_, err := os.Stat(u.TransientPath())
				require.NoError(t, err)

				// read the contents of the transient file.
				tf, err := os.Open(u.TransientPath())
				require.NoError(t, err)
				defer tf.Close()
				bz, err := ioutil.ReadAll(tf)
				require.NoError(t, err)
				require.NoError(t, tf.Close())

				// read the contents of the initial file -> they should match.
				f, err := os.Open("../" + testdata.RootPathCarV1)
				require.NoError(t, err)
				defer f.Close()
				bz2, err := ioutil.ReadAll(f)
				require.NoError(t, err)
				require.NoError(t, f.Close())
				require.EqualValues(t, bz, bz2)
			},
			expectedContentFilePath: "../" + testdata.RootPathCarV1,
		},
		"delete transient": {
			setup: nil,
			createMnt: func(t *testing.T, key string, rootDir string) Mount {
				return &FSMount{testdata.FS, testdata.FSPathCarV2}
			},
			verify: func(t *testing.T, u *Upgrader, key string, rootDir string) {
				ustat, err := u.Stat(context.TODO())
				require.NoError(t, err)

				fstat, err := os.Stat(u.TransientPath())
				require.NoError(t, err)
				require.EqualValues(t, fstat.Size(), ustat.Size)

				err = u.DeleteTransient()
				require.NoError(t, err)

				_, err = os.Stat(u.TransientPath())
				require.Error(t, err)

				require.Empty(t, u.TransientPath())
			},
			expectedContentFilePath: "../" + testdata.RootPathCarV2,
		},
	}

	ctx := context.Background()

	for name, tc := range tcs {
		tcc := tc
		t.Run(name, func(t *testing.T) {
			key := fmt.Sprintf("%d", rand.Uint64())
			rootDir := t.TempDir()
			if tcc.setup != nil {
				tcc.setup(t, key, rootDir)
			}

			mnt := tcc.createMnt(t, key, rootDir)

			u, err := Upgrade(mnt, rootDir, key, tcc.initial)
			require.NoError(t, err)
			require.NotNil(t, u)

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

			tcc.verify(t, u, key, rootDir)
		})
	}
}

func TestUpgraderDeduplicatesRemote(t *testing.T) {
	ctx := context.Background()
	mnt := &Counting{Mount: &FSMount{testdata.FS, testdata.FSPathCarV2}}

	key := fmt.Sprintf("%d", rand.Uint64())
	rootDir := t.TempDir()
	u, err := Upgrade(mnt, rootDir, key, "")
	require.NoError(t, err)
	require.Zero(t, mnt.Count())

	// now fetch in parallel
	cnt := 20
	readers := make([]Reader, cnt)
	grp, _ := errgroup.WithContext(context.Background())
	for i := 0; i < cnt; i++ {
		i := i
		grp.Go(func() error {
			rd, err := u.Fetch(ctx)
			if err != nil {
				return err
			}
			readers[i] = rd
			return nil
		})
	}
	require.NoError(t, grp.Wait())
	// file should have been fetched only once
	require.EqualValues(t, 1, mnt.Count())
	// ensure transient exists
	_, err = os.Stat(u.TransientPath())
	require.NoError(t, err)

	carF, err := os.Open("../" + testdata.RootPathCarV2)
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

	// check transient still exists
	_, err = os.Stat(u.TransientPath())
	require.NoError(t, err)

	// delete the transient
	err = os.Remove(u.TransientPath())
	require.NoError(t, err)

	// fetch again and file should have been fetched twice
	rd, err := u.Fetch(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, mnt.Count())
	_, err = os.Stat(u.TransientPath())
	require.NoError(t, err)

	require.NoError(t, rd.Close())
	_, err = os.Stat(u.TransientPath())
	require.NoError(t, err)
}

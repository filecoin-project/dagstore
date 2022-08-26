package mount

import (
	"bytes"
	"context"
	rand2 "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/dagstore/testdata"
	"github.com/filecoin-project/dagstore/throttle"
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
				fs, err := os.ReadDir(rootDir)
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
				bz, err := io.ReadAll(tf)
				require.NoError(t, err)
				require.NoError(t, tf.Close())

				// read the contents of the initial file -> they should match.
				f, err := os.Open("../" + testdata.RootPathCarV1)
				require.NoError(t, err)
				defer f.Close()
				bz2, err := io.ReadAll(f)
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

			u, err := Upgrade(mnt, throttle.Noop(), rootDir, key, tcc.initial)
			require.NoError(t, err)
			require.NotNil(t, u)

			// fetch and verify contents
			rd, err := u.Fetch(ctx)
			require.NoError(t, err)
			require.NotNil(t, rd)

			bz, err := io.ReadAll(rd)
			require.NoError(t, err)
			require.NotEmpty(t, bz)
			require.NoError(t, rd.Close())

			f, err := os.Open(tcc.expectedContentFilePath)
			require.NoError(t, err)
			bz2, err := io.ReadAll(f)
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
	u, err := Upgrade(mnt, throttle.Noop(), rootDir, key, "")
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
	carBytes, err := io.ReadAll(carF)
	require.NoError(t, err)
	require.NoError(t, carF.Close())

	grp2, _ := errgroup.WithContext(context.Background())
	for _, rd := range readers {
		rdc := rd
		grp2.Go(func() error {
			bz, err := io.ReadAll(rdc)
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

func TestUpgraderFetchAndCopyThrottle(t *testing.T) {
	nFixedThrottle := 3

	tcs := map[string]struct {
		ready                  bool
		expectedThrottledReads int
	}{
		"no throttling when mount is not ready": {
			ready:                  false,
			expectedThrottledReads: 100,
		},
		"throttle when mount is ready": {
			ready:                  true,
			expectedThrottledReads: nFixedThrottle,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			thrt := throttle.Fixed(nFixedThrottle) // same throttle for all
			ctx := context.Background()

			upgraders := make([]*Upgrader, 100)

			underlyings := make([]*blockingReaderMount, 100)
			for i := range upgraders {
				underlyings[i] = &blockingReaderMount{isReady: tc.ready, br: &blockingReader{r: io.LimitReader(rand2.Reader, 1)}}
				u, err := Upgrade(underlyings[i], thrt, t.TempDir(), "foo", "")
				require.NoError(t, err)
				upgraders[i] = u
			}

			// take all locks.
			for _, uu := range underlyings {
				uu.br.lk.Lock()
			}

			errgrp, _ := errgroup.WithContext(ctx)
			for _, u := range upgraders {
				u := u
				errgrp.Go(func() error {
					_, err := u.Fetch(ctx)
					return err
				})
			}

			time.Sleep(500 * time.Millisecond)

			// calls to read across all readers are made without throttling.
			var total int32
			for _, uu := range underlyings {
				total += atomic.LoadInt32(&uu.br.reads)
			}

			require.EqualValues(t, tc.expectedThrottledReads, total)

			// release all locks.
			for _, uu := range underlyings {
				uu.br.lk.Unlock()
			}

			require.NoError(t, errgrp.Wait())

			// we expect 200 calls to read across all readers.
			// 2 per reader: fetching the byte, and the EOF.
			total = 0
			for _, uu := range underlyings {
				total += atomic.LoadInt32(&uu.br.reads)
			}

			require.EqualValues(t, 200, total) // all accessed
		})
	}
}

type blockingReader struct {
	r     io.Reader
	lk    sync.Mutex
	reads int32
}

var _ Reader = (*blockingReader)(nil)

func (br *blockingReader) Close() error {
	return nil
}

func (br *blockingReader) ReadAt(p []byte, off int64) (n int, err error) {
	panic("implement me")
}

func (br *blockingReader) Seek(offset int64, whence int) (int64, error) {
	panic("implement me")
}

func (br *blockingReader) Read(b []byte) (n int, err error) {
	atomic.AddInt32(&br.reads, 1)
	br.lk.Lock()
	defer br.lk.Unlock()
	n, err = br.r.Read(b)
	return n, err
}

type blockingReaderMount struct {
	isReady bool
	br      *blockingReader
}

var _ Mount = (*blockingReaderMount)(nil)

func (b *blockingReaderMount) Close() error {
	return nil
}

func (b *blockingReaderMount) Fetch(ctx context.Context) (Reader, error) {
	return b.br, nil
}

func (b *blockingReaderMount) Info() Info {
	return Info{
		Kind:             KindRemote,
		AccessSequential: true,
	}
}

func (b *blockingReaderMount) Stat(ctx context.Context) (Stat, error) {
	return Stat{
		Exists: true,
		Size:   1024,
		Ready:  b.isReady,
	}, nil
}

func (b *blockingReaderMount) Serialize() *url.URL {
	panic("implement me")
}

func (b *blockingReaderMount) Deserialize(url *url.URL) error {
	panic("implement me")
}

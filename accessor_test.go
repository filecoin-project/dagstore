package dagstore

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/testdata"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/stretchr/testify/require"
)

// TestMmap works on linux and darwin. It tests that for the given mount, if
// multiple accessors are opened, the corresponding file (specified by name)
// will be mmapped or not (depending on expect).
//
// It opens multiple ShardAccessors with Blockstores, so as to create as several
// mmaps. It then closes one accessor, and ensures that the other two are still
// operational (i.e. the shard hasn't been mmapped). Once all accessors are
// closed, it verifies that the shard has been munmap'ped.
//
// Several mount types are tested for their corresponding behaviour. Only mounts
// capable of returning *os.File as a mount.Reader will end up being mmapped.
// Both the FileMount and the Upgrader fall in that category.
func TestMmap(t *testing.T) {
	run := func(t *testing.T, mnt mount.Mount, expect bool, name string) {
		// create three shard accessors, with a blockstore each.
		var accessors []*ShardAccessor
		accessors = append(accessors, createAccessor(t, mnt))
		accessors = append(accessors, createAccessor(t, mnt))
		accessors = append(accessors, createAccessor(t, mnt))

		var bss []ReadBlockstore
		for i := 0; i < 3; i++ {
			bs, err := accessors[i].Blockstore()
			require.NoError(t, err)
			require.NotNil(t, bs)
			bss = append(bss, bs)
		}

		// works.
		blk, err := bss[0].Get(testdata.RootCID)
		require.NoError(t, err)
		require.NotNil(t, blk)

		checkMmapped(t, expect, name)

		// after we close the first accessor, other blockstores are still accessible.
		err = accessors[0].Close()
		require.NoError(t, err)

		// works.
		blk, err = bss[1].Get(testdata.RootCID)
		require.NoError(t, err)
		require.NotNil(t, blk)

		// works.
		blk, err = bss[2].Get(testdata.RootCID)
		require.NoError(t, err)
		require.NotNil(t, blk)

		// still the same mmapped status.
		checkMmapped(t, expect, name)

		// close the other accessors.
		err = accessors[1].Close()
		require.NoError(t, err)

		err = accessors[2].Close()
		require.NoError(t, err)

		// not mmapped any longer, in no case
		checkMmapped(t, false, name)
	}

	// macOS vmmap shows /Users/USER/*/sample-wrapped-v2.car instead of the
	// full path, probably hidden for privacy reasons. So let's match only
	// by filename.
	carv2name := filepath.Base(testdata.RootPathCarV2)

	t.Run("bytes-nommap", func(t *testing.T) {
		// BytesMount doesn't return an *os.File, therefore it's not mmappable.
		mnt := &mount.BytesMount{Bytes: testdata.CarV2}

		run(t, mnt, false, carv2name)
	})

	t.Run("file-mmap", func(t *testing.T) {
		// FileMount does return an *os.File, therefore it's mmappable.
		mnt := &mount.FileMount{Path: testdata.RootPathCarV2}
		run(t, mnt, true, carv2name)
	})

	t.Run("upgrader-mmap", func(t *testing.T) {
		// An upgraded FS mount will mmap its local transient.
		var mnt mount.Mount = &mount.FSMount{FS: testdata.FS, Path: testdata.FSPathCarV2}
		tempdir := t.TempDir()

		var err error
		mnt, err = mount.Upgrade(mnt, tempdir, "foo", "")
		require.NoError(t, err)

		// warm up the upgrader so a transient is created, and we can obtain
		// its path to test against the mmap query output.
		reader, err := mnt.Fetch(context.Background())
		require.NoError(t, err)
		err = reader.Close()
		require.NoError(t, err)

		// in this case, the file we expect to see mmapped is the transient.
		name := filepath.Base(mnt.(*mount.Upgrader).TransientPath())
		run(t, mnt, true, name)
	})

}

// checkMmapped uses platform-specific logic to verify if the expected file
// has been mmapped.
func checkMmapped(t *testing.T, expect bool, name string) {
	pid := os.Getpid()
	pred := require.False
	if expect {
		pred = require.True
	}

	var (
		out []byte
		err error
	)

	// platform-dependent query of process mmaps
	switch runtime.GOOS {
	case "darwin":
		out, err = exec.Command("vmmap", strconv.Itoa(pid)).CombinedOutput()

	case "linux", "openbsd":
		out, err = os.ReadFile("/proc/self/maps")

	default:
		t.Skip("unsupported platform.")
	}

	require.NoError(t, err)

	t.Logf("vmmap or /proc/self/maps output:")
	t.Log(string(out))

	pred(t, strings.Contains(string(out), name))
}

func createAccessor(t *testing.T, mnt mount.Mount) *ShardAccessor {
	dummyShard := &Shard{
		d: &DAGStore{
			ctx:        context.Background(),
			externalCh: make(chan *task, 64),
		},
	}

	reader, err := mnt.Fetch(context.Background())
	require.NoError(t, err)
	defer reader.Close()

	// skip version reading because this API consumes too many bytes.
	// v, err := car.ReadVersion(reader)
	// require.NoError(t, err)
	// require.EqualValues(t, 2, v)
	// skip the pragma instead
	_, err = reader.Seek(car.PragmaSize, 0)
	require.NoError(t, err)

	var h car.Header
	_, err = h.ReadFrom(reader)
	require.NoError(t, err)

	_, err = reader.Seek(int64(h.IndexOffset), 0)
	require.NoError(t, err)

	idx, err := index.ReadFrom(reader)
	require.NoError(t, err)

	reader2, err := mnt.Fetch(context.Background())
	require.NoError(t, err)

	accessor, err := NewShardAccessor(reader2, idx, dummyShard)
	require.NoError(t, err)
	return accessor
}

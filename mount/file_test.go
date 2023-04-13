package mount

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileMount(t *testing.T) {
	const size = 1024

	// create a temp file.
	dir := t.TempDir()
	f, err := ioutil.TempFile(dir, "")
	require.NoError(t, err)
	defer f.Close()

	// read random junk into it up to 1kb; send a copy to a buffer
	// so we can compare.
	var b bytes.Buffer
	r := io.LimitReader(rand.Reader, size)
	r = io.TeeReader(r, &b)
	n, err := io.Copy(f, r)
	require.EqualValues(t, size, n)
	require.NoError(t, err)

	mnt := &FileMount{Path: f.Name()}
	stat, err := mnt.Stat(context.Background())
	require.NoError(t, err)
	require.True(t, stat.Exists)
	require.EqualValues(t, size, stat.Size)

	// check URL.
	require.Equal(t, mnt.Path, mnt.Serialize().Path)

	info := mnt.Info()
	require.True(t, info.AccessSequential && info.AccessSeek && info.AccessRandom) // all flags true
	require.Equal(t, KindLocal, info.Kind)

	reader, err := mnt.Fetch(context.Background())
	require.NoError(t, err)

	// sequential access
	read := make([]byte, 2000)
	i, err := reader.Read(read)
	require.NoError(t, err)
	require.EqualValues(t, size, i) // only truly read 1024 bytes.
	require.Equal(t, b.Bytes(), read[:i])

	// seek to the beginning and read the first byte.
	n, err = reader.Seek(0, 0)
	require.NoError(t, err)
	var b1 [1]byte
	i, err = reader.Read(b1[:])
	require.NoError(t, err)
	require.EqualValues(t, 1, i)
	require.Equal(t, b.Bytes()[0], b1[0])

	// read a random byte.
	i, err = reader.ReadAt(b1[:], 100)
	require.NoError(t, err)
	require.EqualValues(t, 1, i)
	require.Equal(t, b.Bytes()[100], b1[0])

	// close
	err = reader.Close()
	require.NoError(t, err)
}

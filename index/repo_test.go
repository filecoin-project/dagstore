package index

import (
	"bytes"

	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/dagstore/shard"

	"github.com/ipfs/go-cid"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type fullIndexRepoSuite struct {
	suite.Suite
	impl FullIndexRepo
}

func (s *fullIndexRepoSuite) TestAllMethods() {
	r := s.impl
	t := s.T()

	cid1, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaq")
	require.NoError(t, err)
	offset1 := uint64(10)
	k := shard.KeyFromString("shard-key-1")

	// make an index
	idx, err := carindex.New(multicodec.CarIndexSorted)
	require.NoError(t, err)
	err = idx.Load([]carindex.Record{{Cid: cid1, Offset: offset1}})
	require.NoError(t, err)

	// Verify that an empty repo has zero size
	stat, err := r.StatFullIndex(k)
	require.NoError(t, err)
	require.False(t, stat.Exists)
	require.EqualValues(t, 0, stat.Size)

	l, err := r.Len()
	require.NoError(t, err)
	require.EqualValues(t, 0, l)

	size, err := r.Size()
	require.NoError(t, err)
	require.EqualValues(t, 0, size)

	// Verify that there is an error trying to retrieve an index before it's added
	_, err = r.GetFullIndex(k)
	require.Error(t, err)

	// Add an index
	err = r.AddFullIndex(k, idx)
	require.NoError(t, err)

	l, err = r.Len()
	require.NoError(t, err)
	require.EqualValues(t, 1, l)

	// Verify the size of the index is correct
	var b bytes.Buffer
	_, err = carindex.WriteTo(idx, &b)
	require.NoError(t, err)
	expStatSize := b.Len()

	stat, err = r.StatFullIndex(k)
	require.NoError(t, err)
	require.True(t, stat.Exists)
	require.EqualValues(t, expStatSize, stat.Size)

	size, err = r.Size()
	require.NoError(t, err)
	require.EqualValues(t, expStatSize, size)

	count := 0
	err = r.ForEach(func(key shard.Key) (bool, error) {
		if key != k {
			return false, xerrors.Errorf("for each returned wrong key")
		}
		count++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify that we can retrieve an index and perform a lookup
	fidx, err := r.GetFullIndex(k)
	require.NoError(t, err)

	offset, err := carindex.GetFirst(fidx, cid1)
	require.NoError(t, err)
	require.Equal(t, offset1, offset)

	// Drop the index
	dropped, err := r.DropFullIndex(k)
	require.NoError(t, err)
	require.True(t, dropped)

	// Verify that the index is no longer present
	stat, err = r.StatFullIndex(k)
	require.NoError(t, err)
	require.False(t, stat.Exists)
	require.EqualValues(t, 0, stat.Size)

	l, err = r.Len()
	require.NoError(t, err)
	require.EqualValues(t, 0, l)

	size, err = r.Size()
	require.NoError(t, err)
	require.EqualValues(t, 0, size)
}

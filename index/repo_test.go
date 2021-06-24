package index

import (
	"bytes"

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
	offset1 := int64(10)
	k := shard.Key("shard-key-1")
	idx := NewMockFullIndex()
	idx.Set(cid1, offset1)

	// Verify that an empty repo has zero size
	stat, err := r.StatFullIndex(k)
	require.NoError(t, err)
	require.False(t, stat.Exists)
	require.EqualValues(t, 0, stat.Size)

	// Verify that there is an error trying to retrieve an index before it's added
	_, err = r.GetFullIndex(k)
	require.Error(t, err)

	// Add an index
	err = r.AddFullIndex(k, idx)
	require.NoError(t, err)

	// Verify the size of the index is correct
	var b bytes.Buffer
	err = idx.Marshal(&b)
	require.NoError(t, err)
	expStatSize := b.Len()

	stat, err = r.StatFullIndex(k)
	require.NoError(t, err)
	require.True(t, stat.Exists)
	require.EqualValues(t, expStatSize, stat.Size)

	// Verify that we can retrieve an index and perform a lookup
	fidx, err := r.GetFullIndex(k)
	require.NoError(t, err)

	offset, err := fidx.Offset(cid1)
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
}

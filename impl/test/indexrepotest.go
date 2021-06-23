package test

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func RunIndexRepoTest(t *testing.T, r index.FullIndexRepo) {
	cid1, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaq")
	require.NoError(t, err)
	offset1 := int64(10)
	k := shard.Key("shard-key-1")
	idx := NewMockIndex()
	idx.Set(cid1, offset1)

	stat, err := r.StatFullIndex(k)
	require.NoError(t, err)
	require.False(t, stat.Exists)
	require.EqualValues(t, 0, stat.Size)

	_, err = r.GetFullIndex(k)
	require.Error(t, err)

	err = r.AddFullIndex(k, idx)
	require.NoError(t, err)

	var b bytes.Buffer
	err = idx.Marshal(&b)
	require.NoError(t, err)
	expStatSize := b.Len()

	stat, err = r.StatFullIndex(k)
	require.NoError(t, err)
	require.True(t, stat.Exists)
	require.EqualValues(t, expStatSize, stat.Size)

	fidx, err := r.GetFullIndex(k)
	require.NoError(t, err)

	offset, err := fidx.Offset(cid1)
	require.NoError(t, err)
	require.Equal(t, offset1, offset)

	dropped, err := r.DropFullIndex(k)
	require.NoError(t, err)
	require.True(t, dropped)

	stat, err = r.StatFullIndex(k)
	require.NoError(t, err)
	require.False(t, stat.Exists)
	require.EqualValues(t, 0, stat.Size)
}

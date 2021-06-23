package test

import (
	"bytes"
	"testing"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestEmptyMockIndex(t *testing.T) {
	cid1, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaq")
	require.NoError(t, err)

	idx := NewMockIndex()

	len, err := idx.Len()
	require.NoError(t, err)
	require.EqualValues(t, 0, len)

	has, err := idx.Contains(cid1)
	require.NoError(t, err)
	require.False(t, has)

	_, err = idx.Offset(cid1)
	require.Error(t, err)
}

func TestMockIndex(t *testing.T) {
	cid1, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaq")
	require.NoError(t, err)
	offset1 := int64(10)
	cid2, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaa")
	require.NoError(t, err)
	offset2 := int64(20)
	idx := NewMockIndex()

	idx.Set(cid1, offset1)

	len, err := idx.Len()
	require.NoError(t, err)
	require.EqualValues(t, 1, len)

	has, err := idx.Contains(cid1)
	require.NoError(t, err)
	require.True(t, has)

	offset, err := idx.Offset(cid1)
	require.NoError(t, err)
	require.Equal(t, offset1, offset)

	idx.Set(cid2, offset2)

	len, err = idx.Len()
	require.NoError(t, err)
	require.EqualValues(t, 2, len)

	err = idx.ForEach(func(c cid.Cid, offset int64) (bool, error) {
		if c == cid1 && offset != offset1 {
			return false, xerrors.Errorf("expected cid1 offset1")
		}
		if c == cid2 && offset != offset2 {
			return false, xerrors.Errorf("expected cid2 offset2")
		}
		if c != cid1 && c != cid2 {
			return false, xerrors.Errorf("expected cid1 or cid2")
		}
		return true, nil
	})
	require.NoError(t, err)
}

func TestMockIndexMarshaling(t *testing.T) {
	cid1, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaq")
	require.NoError(t, err)
	offset1 := int64(10)
	cid2, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaa")
	require.NoError(t, err)
	offset2 := int64(20)
	idx := NewMockIndex()

	idx.Set(cid1, offset1)
	idx.Set(cid2, offset2)

	var b bytes.Buffer
	err = idx.Marshal(&b)
	require.NoError(t, err)

	idx2 := NewMockIndex()
	err = idx2.Unmarshal(&b)
	require.NoError(t, err)

	len, err := idx.Len()
	require.NoError(t, err)
	require.EqualValues(t, 2, len)
}

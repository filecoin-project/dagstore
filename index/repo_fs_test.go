package index

import (
	"os"
	"testing"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestFSRepo(t *testing.T) {
	basePath := t.TempDir()
	repo, err := NewFSRepo(basePath)
	require.NoError(t, err)

	suite.Run(t, &fullIndexRepoSuite{impl: repo})
}

func TestFSRepoVersions(t *testing.T) {
	basePath := t.TempDir()
	repo, err := NewFSRepo(basePath)
	require.NoError(t, err)

	// Expect the repo to have been initialized with the correct version
	bs, err := os.ReadFile(repo.versionPath())
	require.Equal(t, repoVersion, string(bs))

	// Verify we can create a new repo at the same path
	_, err = NewFSRepo(basePath)
	require.NoError(t, err)

	// Verify that creating a repo at a path with a higher different version
	// returns an error
	err = os.WriteFile(repo.versionPath(), []byte("2"), 0666)
	_, err = NewFSRepo(basePath)
	require.Error(t, err)
}

func TestFSRepoLoadFromDisk(t *testing.T) {
	basePath := t.TempDir()

	cid1, err := cid.Parse("bafykbzaceaeqhm77anl5mv2wjkmh4ofyf6s6eww3ujfmhtsfab65vi3rlccaq")
	require.NoError(t, err)
	offset1 := uint64(10)
	k := shard.KeyFromString("shard-key-1")

	// make an index
	idx, err := carindex.New(multicodec.CarIndexSorted)
	require.NoError(t, err)
	err = idx.Load([]carindex.Record{{Cid: cid1, Offset: offset1}})
	require.NoError(t, err)

	// Create a repo at the base path
	repo1, err := NewFSRepo(basePath)
	require.NoError(t, err)

	// Add an index to the repo
	err = repo1.AddFullIndex(k, idx)
	require.NoError(t, err)

	// Create a new repo at the same path
	repo2, err := NewFSRepo(basePath)
	require.NoError(t, err)

	// Verify that we can get the index from the repo and do a lookup
	fidx, err := repo2.GetFullIndex(k)
	require.NoError(t, err)

	offset, err := carindex.GetFirst(fidx, cid1)
	require.NoError(t, err)
	require.Equal(t, offset1, offset)
}

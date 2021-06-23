package index

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestLocalIndexRepo(t *testing.T) {
	basePath := t.TempDir()
	repo, err := NewFSRepo(basePath)
	require.NoError(t, err)

	suite.Run(t, &fullIndexRepoSuite{impl: repo})
}

package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type indexFactory struct {
}

func (i indexFactory) Build(path string) (ix FullIndex, err error) {
	idx := NewMockFullIndex()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	err = idx.Unmarshal(f)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

var _ FullIndexFactory = (*indexFactory)(nil)

func TestLocalIndexRepo(t *testing.T) {
	factory := indexFactory{}
	basePath := t.TempDir()
	repo, err := NewFSRepo(factory, basePath)
	require.NoError(t, err)

	suite.Run(t, &fullIndexRepoSuite{impl: repo})
}

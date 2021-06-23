package local

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/dagstore/index"

	"github.com/filecoin-project/dagstore/impl/test"
)

type indexFactory struct {
}

func (i indexFactory) Build(path string) (ix index.FullIndex, err error) {
	idx := test.NewMockIndex()

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
	repo, err := NewLocalIndexRepo(factory, basePath)
	require.NoError(t, err)
	test.RunIndexRepoTest(t, repo)
}

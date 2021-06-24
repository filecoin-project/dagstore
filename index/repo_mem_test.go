package index

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestMemIndexRepo(t *testing.T) {
	suite.Run(t, &fullIndexRepoSuite{impl: NewMemoryRepo()})
}

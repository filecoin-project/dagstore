package mem

import (
	"testing"

	"github.com/filecoin-project/dagstore/impl/test"
)

func TestMemIndexRepo(t *testing.T) {
	test.RunIndexRepoTest(t, NewMemIndexRepo())
}

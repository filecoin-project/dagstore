package index

import (
	"testing"
)

func TestMemIndexRepo(t *testing.T) {
	runFullIndexRepoTest(t, NewMemory())
}

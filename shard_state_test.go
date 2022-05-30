package dagstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardStates(t *testing.T) {
	ss := ShardStateAvailable
	require.Equal(t, "ShardStateAvailable", ss.String())

	ss = ShardState(201)
	require.Equal(t, "", ss.String())
}

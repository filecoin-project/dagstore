package dagstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardStates(t *testing.T) {
	ss := ShardStateRecovering
	require.Equal(t, "ShardStateRecovering", ss.String())

	ss = ShardStateInitializing
	require.Equal(t, "ShardStateInitializing", ss.String())

	ss = ShardState(201)
	require.Equal(t, "", ss.String())
}

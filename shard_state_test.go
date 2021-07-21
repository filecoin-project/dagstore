package dagstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardStates(t *testing.T) {
	ss := ShardState(ShardStateRecovering)
	require.Equal(t, "ShardStateRecovering", ss.String())

	ss = ShardState(ShardStateInitializing)
	require.Equal(t, "ShardStateInitializing", ss.String())

	ss = ShardState(201)
	require.Equal(t, "unknown shard state", ss.String())
}

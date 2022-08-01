package gc

import (
	"testing"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/stretchr/testify/require"
)

func TestLRUGarbageCollector(t *testing.T) {
	lru := NewLRUGarbageCollector()

	sk1 := shard.KeyFromString("key")
	sk2 := shard.KeyFromString("key2")
	sk3 := shard.KeyFromString("key3")

	lru.NotifyReclaimable(sk1)
	require.Equal(t, []shard.Key{sk1}, lru.Reclaimable())

	lru.NotifyReclaimable(sk2)
	lru.NotifyAccessed(sk1)
	require.Equal(t, []shard.Key{sk2, sk1}, lru.Reclaimable())

	lru.NotifyAccessed(sk2)
	require.Equal(t, []shard.Key{sk1, sk2}, lru.Reclaimable())

	lru.NotifyNotReclaimable(sk1)
	require.Equal(t, []shard.Key{sk2}, lru.Reclaimable())

	lru.NotifyReclaimable(sk3)
	require.Equal(t, []shard.Key{sk3, sk2}, lru.Reclaimable())

	lru.NotifyReclaimable(sk1)
	require.Equal(t, sk2, lru.Reclaimable()[2])

	lru.NotifyAccessed(sk3)
	require.Equal(t, []shard.Key{sk1, sk2, sk3}, lru.Reclaimable())

	lru.NotifyRemoved(sk2)
	require.Equal(t, []shard.Key{sk1, sk3}, lru.Reclaimable())

	lru.NotifyReclaimed([]shard.Key{sk1})
	require.Equal(t, []shard.Key{sk3}, lru.Reclaimable())

	lru.NotifyReclaimed([]shard.Key{sk1, sk3})
	require.Equal(t, []shard.Key{}, lru.Reclaimable())
}

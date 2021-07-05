package dagstore

import (
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
)

// Shard encapsulates the state of a shard within the DAG store.
type Shard struct {
	// IMMUTABLE FIELDS: safe to read outside the event loop.
	key   shard.Key
	mount *mount.Upgrader

	// MUTABLE FIELDS: cannot read/write outside event loop.
	wRegister chan ShardResult
	wAcquire  []chan ShardResult
	wDestroy  chan ShardResult // TODO implement destroy wait

	state   ShardState
	err     error // populated if shard state is errored.
	indexed bool

	refs uint32 // count of DAG accessors currently open
}

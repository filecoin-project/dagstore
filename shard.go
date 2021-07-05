package dagstore

import (
	"context"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
)

// waiter encapsulates a context passed by the user, and the channel they want
// the result returned to.
type waiter struct {
	ctx   context.Context
	outCh chan ShardResult
}

func (w waiter) dispatch(res ShardResult) {
	select {
	case <-w.ctx.Done():
	case w.outCh <- res:
	}
}

// Shard encapsulates the state of a shard within the DAG store.
type Shard struct {
	// IMMUTABLE FIELDS: safe to read outside the event loop.
	key   shard.Key
	mount *mount.Upgrader

	// MUTABLE FIELDS: cannot read/write outside event loop.
	state   ShardState
	err     error // populated if shard state is errored.
	indexed bool

	wRegister *waiter
	wAcquire  []*waiter
	wDestroy  *waiter

	refs uint32 // count of DAG accessors currently open
}

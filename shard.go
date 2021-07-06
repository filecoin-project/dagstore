package dagstore

import (
	"context"
	"sync"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
)

// waiter encapsulates a context passed by the user, and the channel they want
// the result returned to.
type waiter struct {
	// context governing the operation if this is an external op.
	ctx   context.Context
	outCh chan ShardResult
}

func (w waiter) deliver(res *ShardResult) {
	select {
	case w.outCh <- *res:
	case <-w.ctx.Done():
	}
}

// Shard encapsulates the state of a shard within the DAG store.
type Shard struct {
	lk sync.RWMutex

	// IMMUTABLE FIELDS: safe to read outside the event loop without a lock.
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

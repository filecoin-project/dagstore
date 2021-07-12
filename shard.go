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
	ctx   context.Context    // governs the op if it's external
	outCh chan<- ShardResult // to send back the result
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

	// Immutable fields.
	// Safe to read outside the event loop without a lock.
	d     *DAGStore       // backreference
	key   shard.Key       // persisted in PersistedShard.Key
	mount *mount.Upgrader // persisted in PersistedShard.URL (underlying)
	lazy  bool            // persisted in PersistedShard.Lazy; whether this shard has lazy indexing

	// Mutable fields.
	// Cannot read/write outside event loop.
	state ShardState // persisted in PersistedShard.State
	err   error      // persisted in PersistedShard.Error; populated if shard state is errored.

	// Waiters.
	wRegister *waiter   // waiter for registration result.
	wAcquire  []*waiter // waiters for acquiring the shard.
	wDestroy  *waiter   // waiter for shard destruction.

	refs uint32 // number of DAG accessors currently open
}

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
	ctx        context.Context    // governs the op if it's external
	outCh      chan<- ShardResult // to send back the result
	notifyDead func()             // called when the context expired and we weren't able to deliver the result
}

func (w waiter) deliver(res *ShardResult) {
	if w.outCh == nil {
		return
	}
	select {
	case w.outCh <- *res:
	case <-w.ctx.Done():
		if w.notifyDead != nil {
			w.notifyDead()
		}
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

	recoverOnNextAcquire bool // a shard marked in error state during initialization can be recovered on its first acquire.

	// Waiters.
	wRegister *waiter   // waiter for registration result.
	wRecover  *waiter   // waiter for recovering an errored shard.
	wAcquire  []*waiter // waiters for acquiring the shard.
	wDestroy  *waiter   // waiter for shard destruction.

	refs uint32 // number of DAG accessors currently open
}

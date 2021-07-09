package dagstore

import (
	"context"
	"fmt"
)

type OpType int

const (
	OpShardRegister OpType = iota
	OpShardMakeAvailable
	OpShardDestroy
	OpShardAcquire
	OpShardFail
	OpShardRelease

	opFlush // only for tests
)

func (o OpType) String() string {
	return [...]string{
		"OpShardRegister",
		"OpShardMakeAvailable",
		"OpShardDestroy",
		"OpShardAcquire",
		"OpShardFail",
		"OpShardRelease",
		"opFlush"}[o]
}

// control runs the DAG store's event loop.
func (d *DAGStore) control() {
	defer d.wg.Done()

	var (
		tsk *task
		err error
	)

	for {
		if tsk, err = d.consumeNext(); err != nil {
			break
		}
		log.Debugw("processing task", "op", tsk.op, "shard", tsk.shard.key, "error", tsk.err)

		s := tsk.shard
		s.lk.Lock()

		switch tsk.op {
		case OpShardRegister:
			if s.state != ShardStateNew {
				// sanity check failed
				err := fmt.Errorf("%w: expected shard to be in 'new' state; was: %d", ErrShardInitializationFailed, s.state)
				_ = d.queueTask(&task{op: OpShardFail, shard: tsk.shard, err: err}, d.internalCh)
				break
			}

			s.state = ShardStateInitializing
			// if we already have the index for this shard, there's nothing to do here.
			if istat, err := d.indices.StatFullIndex(s.key); err == nil && istat.Exists {
				_ = d.queueTask(&task{op: OpShardMakeAvailable, shard: s}, d.internalCh)
				break
			}
			// otherwise, generate and persist an Index for the CAR payload of the given Shard.
			go d.initializeAsync(tsk.ctx, s, s.mount)

		case OpShardMakeAvailable:
			if s.wRegister != nil {
				res := &ShardResult{Key: s.key}
				d.sendResult(res, s.wRegister)
				s.wRegister = nil
			}

			s.state = ShardStateAvailable

			// trigger queued acquisition waiters.
			for _, w := range s.wAcquire {
				// optimistically increment the refcount to acquire the shard. The go-routine will send an `OpShardRelease` message
				// to the event loop if it fails to acquire the shard.
				s.refs++
				go d.acquireAsync(tsk.ctx, w, s, s.mount)
			}
			s.wAcquire = s.wAcquire[:0]

		case OpShardAcquire:
			w := &waiter{ctx: tsk.ctx, outCh: tsk.outCh}
			// TODO What if Shard is in the ShardStateErrored state  here ? For now, that will block the acquirer as
			// we add the acquirer to the WaitQueue here.

			// We should probably disallow all ops for a shards in
			// the errored state except for the recover op. ?

			if s.state != ShardStateAvailable {
				// shard state isn't active yet; make this acquirer wait.
				s.wAcquire = append(s.wAcquire, w)
				break
			}

			// optimistically increment the refcount to acquire the shard. The go-routine will send an `OpShardRelease` message
			// to the event loop if it fails to acquire the shard.
			s.refs++
			go d.acquireAsync(tsk.ctx, w, s, s.mount)

		case OpShardRelease:
			if (s.state != ShardStateAvailable && s.state != ShardStateErrored) || s.refs <= 0 {
				log.Warn("ignored illegal request to release shard")
				break
			}
			s.refs--

		case OpShardFail:
			s.state = ShardStateErrored
			s.err = tsk.err

			// can't block the event loop, so launch a goroutine to notify.
			if s.wRegister != nil {
				res := &ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to register shard: %w", tsk.err),
				}
				d.sendResult(res, s.wRegister)
			}

			// fail waiting acquirers.
			// can't block the event loop, so launch a goroutine per acquirer.
			if len(s.wAcquire) > 0 {
				err := fmt.Errorf("failed to acquire shard: %w", tsk.err)
				res := &ShardResult{Key: s.key, Error: err}
				d.sendResult(res, s.wAcquire...)
				s.wAcquire = s.wAcquire[:0] // clear acquirers.
			}

			// TODO What about all those who have already acquired
			// TODO notify application.

		case OpShardDestroy:
			if s.refs > 0 {
				err := fmt.Errorf("failed to destroy shard; active references: %d", s.refs)
				res := &ShardResult{Key: s.key, Error: err}
				d.sendResult(res, tsk.waiter)
				break
			}

			d.lk.Lock()
			delete(d.shards, s.key)
			d.lk.Unlock()
			// TODO are we guaranteed that there are no queued items for this shard?

		case opFlush:
			res := &ShardResult{Key: s.key, Error: nil}
			d.sendResult(res, tsk.waiter)
		}

		// persist the current shard state.
		if err := s.persist(d.config.Datastore); err != nil { // TODO maybe fail shard?
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}

		s.lk.Unlock()
	}

	if err != context.Canceled {
		log.Errorw("consuming next task failed; aborted event loop; dagstore unoperational", "error", err)
	}
}

func (d *DAGStore) consumeNext() (tsk *task, error error) {
	select {
	case tsk = <-d.internalCh: // drain internal first; these are tasks emitted from the event loop.
		return tsk, nil
	case <-d.ctx.Done():
		return nil, d.ctx.Err() // TODO drain and process before returning?
	default:
	}

	select {
	case tsk = <-d.externalCh:
		return tsk, nil
	case tsk = <-d.completionCh:
		return tsk, nil
	case <-d.ctx.Done():
		return nil, d.ctx.Err() // TODO drain and process before returning?
	}
}

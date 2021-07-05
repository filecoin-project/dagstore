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
	OpAllShardsInfo
)

func (o OpType) String() string {
	return [...]string{
		"OpShardRegister",
		"OpShardMakeAvailable",
		"OpShardDestroy",
		"OpShardAcquire",
		"OpShardFail",
		"OpShardRelease",
		"OpAllShardsInfo"}[o]
}

// control runs the DAG store's event loop.
func (d *DAGStore) control() {
	defer d.wg.Done()

	tsk, err := d.consumeNext()
	for ; err == nil; tsk, err = d.consumeNext() {
		log.Debugw("processing task", "op", tsk.op, "shard", tsk.shard.key, "error", tsk.err)

		s := tsk.shard
		switch tsk.op {
		case OpShardRegister:
			if s.state != ShardStateNew {
				// sanity check failed
				err := fmt.Errorf("%w: expected shard to be in 'new' state; was: %d", ErrShardInitializationFailed, s.state)
				_ = d.queueTask(&task{op: OpShardFail, shard: tsk.shard, err: err}, d.internalCh)
				break
			}

			s.state = ShardStateInitializing

			go d.initializeAsync(tsk.ctx, s, s.mount)

		case OpShardMakeAvailable:
			if s.wRegister != nil {
				res := &Result{Key: s.key}
				d.sendResult(res, s.wRegister)
				s.wRegister = nil
			}

			s.state = ShardStateAvailable

			// trigger queued acquisition waiters.
			for _, w := range s.wAcquire {
				s.refs++
				go d.acquireAsync(tsk.ctx, w, s, s.mount)
			}
			s.wAcquire = s.wAcquire[:0]

		case OpShardAcquire:
			w := &waiter{ctx: tsk.ctx, outCh: tsk.outCh}
			if s.state != ShardStateAvailable && s.state != ShardStateServing {
				// shard state isn't active yet; make this acquirer wait.
				s.wAcquire = append(s.wAcquire, w)
				break
			}

			s.state = ShardStateServing
			s.refs++

			go d.acquireAsync(tsk.ctx, w, s, s.mount)

		case OpShardRelease:
			if (s.state != ShardStateServing && s.state != ShardStateErrored) || s.refs <= 0 {
				log.Warnf("ignored illegal request to release shard")
				break
			}
			s.refs--

			if s.refs == 0 {
				s.state = ShardStateAvailable
			}

		case OpShardFail:
			s.state = ShardStateErrored
			s.err = tsk.err

			// can't block the event loop, so launch a goroutine to notify.
			if s.wRegister != nil {
				res := &Result{
					Key:   s.key,
					Error: fmt.Errorf("failed to register shard: %w", tsk.err),
				}
				d.sendResult(res, s.wRegister)
			}

			// fail waiting acquirers.
			// can't block the event loop, so launch a goroutine per acquirer.
			if len(s.wAcquire) > 0 {
				err := fmt.Errorf("failed to acquire shard: %w", tsk.err)
				res := &Result{Key: s.key, Error: err}
				d.sendResult(res, s.wAcquire...)
				s.wAcquire = s.wAcquire[:0] // clear acquirers.
			}

			// TODO notify application.

		case OpShardDestroy:
			if s.state == ShardStateServing || s.refs > 0 {
				err := fmt.Errorf("failed to destroy shard; active references: %d", s.refs)
				res := &Result{Key: s.key, Error: err}
				d.sendResult(res, tsk.waiter)
				break
			}

			d.lk.Lock()
			delete(d.shards, s.key)
			d.lk.Unlock()
			// TODO are we guaranteed that there are no queued items for this shard?

		case OpAllShardsInfo:
			// TODO not sure I like taking the global lock; all this feels very hacky.
			d.lk.RLock()
			info := make(AllShardsInfo, len(d.shards))
			for k, v := range d.shards {
				info[k] = ShardInfo{
					ShardState: v.state,
					Error:      v.err,
				}
			}
			d.lk.RUnlock()
			res := &Result{respAllShardsInfo: info}
			d.sendResult(res, tsk.waiter)
		}

		// persist the current shard state.
		if err := s.persist(d.config.Datastore); err != nil { // TODO maybe fail shard?
			log.Warnw("failed to persist shard", "shard", s.key, "error", err)
		}
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
		return // TODO drain and process before returning?
	}
}

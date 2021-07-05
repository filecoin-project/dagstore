package dagstore

import (
	"context"
	"fmt"
)

// control runs the DAG store's event loop.
func (d *DAGStore) control() {
	defer d.wg.Done()

	tsk, err := d.consumeNext()
	for ; err == nil; tsk, err = d.consumeNext() {
		// TODO lower to debug before release
		log.Infow("processing task", "op", tsk.Op, "shard", tsk.Shard.key, "error", tsk.Error)

		switch s := tsk.Shard; tsk.Op {
		case OpShardRegister:
			if s.state != ShardStateNew {
				// sanity check failed
				err := fmt.Errorf("%w: expected shard to be in 'new' state; was: %d", ErrShardInitializationFailed, s.state)
				_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, d.internalCh)
				break
			}

			s.state = ShardStateInitializing

			go d.initializeAsync(tsk.Ctx, s, s.mount)

		case OpShardMakeAvailable:
			if s.wRegister != nil {
				res := ShardResult{Key: s.key}
				go func(ch chan ShardResult) { ch <- res }(s.wRegister)
				s.wRegister = nil
			}

			s.state = ShardStateAvailable

			// trigger queued acquisition waiters.
			for _, acqCh := range s.wAcquire {
				s.refs++
				go d.acquireAsync(tsk.Ctx, acqCh, s, s.mount)
			}
			s.wAcquire = s.wAcquire[:0]

		case OpShardAcquire:
			if s.state != ShardStateAvailable && s.state != ShardStateServing {
				// shard state isn't active yet; make this acquirer wait.
				s.wAcquire = append(s.wAcquire, tsk.Resp)
				break
			}

			s.state = ShardStateServing
			s.refs++
			go d.acquireAsync(tsk.Ctx, tsk.Resp, s, s.mount)

		case OpShardRelease:
			if (s.state != ShardStateServing && s.state != ShardStateErrored) || s.refs <= 0 {
				log.Warnf("ignored illegal request to release shard")
				break
			}
			s.refs--

		case OpShardFail:
			s.state = ShardStateErrored
			s.err = tsk.Error

			// can't block the event loop, so launch a goroutine to notify.
			if s.wRegister != nil {
				res := ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to register shard: %w", tsk.Error),
				}
				go func(ch chan ShardResult) { ch <- res }(s.wRegister)
			}

			// fail waiting acquirers.
			// can't block the event loop, so launch a goroutine per acquirer.
			if len(s.wAcquire) > 0 {
				res := ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to acquire shard: %w", tsk.Error),
				}
				for _, acqCh := range s.wAcquire {
					go func(ch chan ShardResult) { ch <- res }(acqCh)
				}
				s.wAcquire = s.wAcquire[:0] // empty acquirers.
			}

			// TODO notify application.

		case OpShardDestroy:
			if s.state == ShardStateServing || s.refs > 0 {
				res := ShardResult{
					Key:   s.key,
					Error: fmt.Errorf("failed to destroy shard; active references: %d", s.refs),
				}
				go func(ch chan ShardResult) { ch <- res }(tsk.Resp)
				break
			}

			d.lk.Lock()
			delete(d.shards, s.key)
			d.lk.Unlock()
			// TODO are we guaranteed that there are no queued items for this shard?
		}
	}

	if err != context.Canceled {
		log.Errorw("consuming next task failed; aborted event loop; dagstore unoperational", "error", err)
	}
}

func (d *DAGStore) consumeNext() (tsk *Task, error error) {
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

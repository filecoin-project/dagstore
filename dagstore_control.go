package dagstore

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore/mount"
)

// control runs the DAG store's event loop.
func (d *DAGStore) control() {
	defer d.wg.Done()

	tsk, err := d.consumeNext()
	for ; err == nil; tsk, err = d.consumeNext() {
		// TODO lower to debug before release
		log.Infow("processing task", "op", tsk.Op, "shard", tsk.Shard.key)

		s := tsk.Shard

		switch tsk.Op {
		case OpShardRegister:
			if s.state != ShardStateNew {
				err := fmt.Errorf("%w: expected shard to be in 'new' state; was: %d", ErrShardInitializationFailed, s.state)
				_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, d.internalCh)
				break
			}
			// queue a fetch.
			_ = d.queueTask(&Task{Op: OpShardFetch, Shard: tsk.Shard}, d.internalCh)

		case OpShardFetch:
			s.state = ShardStateFetching

			go func(ctx context.Context, upgrader *mount.Upgrader) {
				// ensure a copy is available locally.
				reader, err := upgrader.Fetch(ctx)
				if err != nil {
					err = fmt.Errorf("failed to acquire reader of mount: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, d.completionCh)
					return
				}
				_ = reader.Close()
				_ = d.queueTask(&Task{Op: OpShardFetchDone, Shard: tsk.Shard}, d.completionCh)
			}(d.ctx, s.mount)

		case OpShardFetchDone:
			s.state = ShardStateFetched
			if !s.indexed {
				// shard isn't indexed yet, so let's index.
				_ = d.queueTask(&Task{Op: OpShardIndex, Shard: tsk.Shard}, d.internalCh)
				break
			}
			// shard is indexed, we're ready to serve requests.
			_ = d.queueTask(&Task{Op: OpShardMakeAvailable, Shard: tsk.Shard}, d.internalCh)

		case OpShardIndex:
			s.state = ShardStateIndexing
			go func(ctx context.Context, mnt mount.Mount) {
				reader, err := mnt.Fetch(ctx)
				if err != nil {
					err = fmt.Errorf("failed to acquire reader of mount: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, d.completionCh)
					return
				}
				defer reader.Close()

				idx, err := loadIndexAsync(reader)
				if err != nil {
					err = fmt.Errorf("failed to index shard: %w", err)
					_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, d.completionCh)
					return
				}
				_ = d.queueTask(&Task{Op: OpShardIndexDone, Shard: tsk.Shard, Index: idx}, d.completionCh)
			}(d.ctx, s.mount)

		case OpShardIndexDone:
			err := d.indices.AddFullIndex(s.key, tsk.Index)
			if err != nil {
				err = fmt.Errorf("failed to add index for shard: %w", err)
				_ = d.queueTask(&Task{Op: OpShardFail, Shard: tsk.Shard, Error: err}, d.internalCh)
				break
			}
			_ = d.queueTask(&Task{Op: OpShardMakeAvailable, Shard: tsk.Shard}, d.internalCh)

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
				go d.acquireAsync(acqCh, s, s.mount)
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
			go d.acquireAsync(tsk.Resp, s, s.mount)

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

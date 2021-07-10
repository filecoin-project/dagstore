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
				_ = d.failShard(s, d.internalCh, "%w: expected shard to be in 'new' state; was: %d", ErrShardInitializationFailed, s.state)
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

			// if the shard is errored, fail the acquire immediately.
			// TODO requires test
			if s.state == ShardStateErrored {
				err := fmt.Errorf("shard is in errored state; err: %w", s.err)
				res := &ShardResult{Key: s.key, Error: err}
				d.sendResult(res, s.wRegister)
				break
			}

			if s.state != ShardStateAvailable && s.state != ShardStateServing {
				// shard state isn't active yet; make this acquirer wait.
				s.wAcquire = append(s.wAcquire, w)
				break
			}

			// mark as serving.
			s.state = ShardStateServing

			// optimistically increment the refcount to acquire the shard.
			// The goroutine will send an `OpShardRelease` task
			// to the event loop if it fails to acquire the shard.
			s.refs++
			go d.acquireAsync(tsk.ctx, w, s, s.mount)

		case OpShardRelease:
			if (s.state != ShardStateServing && s.state != ShardStateErrored) || s.refs <= 0 {
				log.Warn("ignored illegal request to release shard")
				break
			}

			// decrement refcount.
			s.refs--

			// reset state back to available, if we were the last
			// active acquirer.
			if s.refs == 0 {
				s.state = ShardStateAvailable
			}

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

			// Should we interrupt/disturb active acquirers? No.
			//
			// This part doesn't know which kind of error occurred.
			// It could be that the index has disappeared for new acquirers, but
			// active acquirers already have it.
			//
			// If this is a physical error (e.g. shard data was physically
			// deleted, or corrupted), we'll leave to the ShardAccessor (and the
			// ReadBlockstore) to fail at some point. At that stage, the caller
			// will call ShardAccessor#Close and eventually all active
			// references will be released, setting the shard in an errored
			// state with zero refcount.

			// TODO notify application.

		case OpShardDestroy:
			if s.state == ShardStateServing || s.refs > 0 {
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

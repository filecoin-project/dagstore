package dagstore

// dispatcher takes care of dispatching results back to the application.
//
// These can be results of API operations, or shard failures.
func (d *DAGStore) dispatcher(ch chan *dispatch) {
	defer d.wg.Done()

	var di *dispatch
	for {
		select {
		case di = <-ch:
		case <-d.ctx.Done():
			return
		}
		di.w.deliver(di.res)
	}
}

func (d *DAGStore) dispatchResult(res *ShardResult, waiters ...*waiter) {
	for _, w := range waiters {
		if w.outCh == nil {
			// no return channel; skip.
			continue
		}
		d.dispatchResultsCh <- &dispatch{w: w, res: res}
	}
}

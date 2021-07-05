package dagstore

// dispatcher takes care of pumping results back to callers.
func (d *DAGStore) dispatcher() {
	defer d.wg.Done()

	var di *dispatch
	for {
		select {
		case di = <-d.dispatchCh:
		case <-d.ctx.Done():
			return
		}
		di.w.deliver(di.res)
	}
}

func (d *DAGStore) sendResult(res *Result, waiters ...*waiter) {
	for _, w := range waiters {
		d.dispatchCh <- &dispatch{w: w, res: res}
	}
}

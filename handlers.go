package dagstore

import (
	"context"

	"github.com/filecoin-project/dagstore/shard"
)

// RecoverImmediately takes a failureCh where DAGStore failures are sent, and
// attempts to recover the shard immediately up until maxAttempts for each
// unique shard.
//
// Attempt tracking does not survive restarts. When the passed context fires,
// the failure handler will yield and the given `onDone` function is called before returning. It is recommended to call this
// method from a dedicated goroutine, as it runs an infinite event
// loop.
func RecoverImmediately(ctx context.Context, dagst *DAGStore, failureCh chan ShardResult, maxAttempts uint64, onDone func()) {
	if onDone != nil {
		defer onDone()
	}
	var (
		recResCh = make(chan ShardResult, 128)
		attempts = make(map[shard.Key]uint64)
	)

	for {
		select {
		case res := <-failureCh:
			key := res.Key
			att := attempts[key]
			if att >= maxAttempts {
				log.Infow("failure handler: max attempts exceeded; skipping recovery", "key", key, "from_error", res.Error, "attempt", att)
				continue
			}

			log.Infow("failure handler: recovering shard", "key", key, "from_error", res.Error, "attempt", att)

			// queue the recovery for this key.
			if err := dagst.RecoverShard(ctx, key, recResCh, RecoverOpts{}); err != nil {
				log.Warnw("failure handler: failed to queue shard recovery", "key", key, "error", err)
				continue
			}
			attempts[key]++

		case res := <-recResCh:
			// this channel is just informational; a failure to recover will
			// trigger another failure on the failureCh, which will be handled
			// above for retry.
			key := res.Key
			if res.Error == nil {
				log.Infow("failure handler: successfully recovered shard", "key", key)
				delete(attempts, key)
			} else {
				log.Warnw("failure handler: failed to recover shard", "key", key, "attempt", attempts[key])
			}
			continue

		case <-ctx.Done():
			log.Info("failure handler: stopping")
			attempts = nil
			return
		}
	}
}

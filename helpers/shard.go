package helpers

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
)

// AcquireShardSync blocks until the requested shard is acquired or the given context expires.
// It returns the acquired shard if the acquire was successful. It is the caller's responsibility
// to close the returned accessor when done.
func AcquireShardSync(ctx context.Context, dagst *dagstore.DAGStore, sk shard.Key, opts dagstore.AcquireOpts) (*dagstore.ShardAccessor, error) {
	ch := make(chan dagstore.ShardResult, 1)

	if err := dagst.AcquireShard(ctx, sk, ch, opts); err != nil {
		return nil, fmt.Errorf("failed to acquire shard: %w", err)
	}

	var res dagstore.ShardResult
	select {
	case res = <-ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if err := res.Error; err != nil {
		return nil, fmt.Errorf("failed to acquire shard: %w", err)
	}

	return res.Accessor, nil
}

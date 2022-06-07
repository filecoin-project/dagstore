package helpers

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
)

func AcquireShardSync(ctx context.Context, dagst *dagstore.DAGStore, sk shard.Key) (*dagstore.ShardAccessor, error) {
	ch := make(chan dagstore.ShardResult, 1)

	if err := dagst.AcquireShard(ctx, sk, ch, dagstore.AcquireOpts{}); err != nil {
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

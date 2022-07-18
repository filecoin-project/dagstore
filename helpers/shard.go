package helpers

import (
	"context"
	"fmt"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/dagstore/mount"

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

// RegisterAndAcquireSync attempts to register the shard if it's not already registered and then proceeds to synchronously acquire the shard.
// It is the caller's responsibility to close the returned accessor when done.
func RegisterAndAcquireSync(ctx context.Context, dagst *dagstore.DAGStore, key shard.Key, mnt mount.Mount, ropts dagstore.RegisterOpts,
	aopts dagstore.AcquireOpts) (*dagstore.ShardAccessor, error) {
	if err := dagst.RegisterShard(ctx, key, mnt, nil, ropts); err != nil && !xerrors.Is(err, dagstore.ErrShardExists) {
		return nil, fmt.Errorf("failed to register shard: %w", err)
	}
	return AcquireShardSync(ctx, dagst, key, aopts)
}

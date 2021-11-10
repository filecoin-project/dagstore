package dagstore

import (
	"context"

	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
)

// Interface is the publicly exposed interface of the DAGStore. It exists
// for mocking or DI purposes.
type Interface interface {
	Start(ctx context.Context) error
	RegisterShard(ctx context.Context, key shard.Key, mnt mount.Mount, out chan ShardResult, opts RegisterOpts) error
	DestroyShard(ctx context.Context, key shard.Key, out chan ShardResult, _ DestroyOpts) error
	AcquireShard(ctx context.Context, key shard.Key, out chan ShardResult, _ AcquireOpts) error
	RecoverShard(ctx context.Context, key shard.Key, out chan ShardResult, _ RecoverOpts) error
	GetShardInfo(k shard.Key) (ShardInfo, error)
	AllShardsInfo() AllShardsInfo
	ShardsContainingMultihash(h mh.Multihash) ([]shard.Key, error)
	GC(ctx context.Context) (*GCResult, error)
	Close() error
}

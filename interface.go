package dagstore

import (
	"context"

	"github.com/ipfs/go-cid"

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
	GetShardKeysForCid(c cid.Cid) ([]shard.Key, error)
	GC(ctx context.Context) (*GCResult, error)
	Close() error
}

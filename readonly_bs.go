package dagstore

import (
	"context"
	"errors"
	"fmt"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/dagstore/shard"
	lru "github.com/hashicorp/golang-lru"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var logbs = logging.Logger("dagstore_all_bs")

var _ blockstore.Blockstore = (*AllShardsReadBlockstore)(nil)

type ShardSelectorF func(c cid.Cid, shards []shard.Key) (shard.Key, error)

type accessorWithBlockstore struct {
	sa *ShardAccessor
	bs ReadBlockstore
}

type AllShardsReadBlockstore struct {
	d            *DAGStore
	shardSelectF ShardSelectorF

	// caches the carV1 payload stream and the carv2 index for shard read affinity i.e. further reads will likely be from the same shard.
	// shard key -> read only blockstore (CARV1 stream + CARv2 Index)
	bsCache *lru.Cache

	// caches the blocks themselves -> can be scaled by using a redis/memcache etc distributed cache
	// multihash -> block
	blkCache *lru.ARCCache
}

func (d *DAGStore) AllShardsReadBlockstore(shardSelector ShardSelectorF, maxCacheSize int, maxBlocks int) (blockstore.Blockstore, error) {
	bslru, err := lru.NewWithEvict(maxCacheSize, func(_ interface{}, val interface{}) {
		abs := val.(*accessorWithBlockstore)
		abs.sa.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for read only blockstores")
	}
	blkLru, err := lru.NewARC(maxBlocks)
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for blocks: %w", err)
	}

	return &AllShardsReadBlockstore{
		d:            d,
		shardSelectF: shardSelector,
		bsCache:      bslru,
		blkCache:     blkLru,
	}, nil

}

func (ro *AllShardsReadBlockstore) Get(ctx context.Context, c cid.Cid) (b blocks.Block, finalErr error) { // get all the shards containing the mh
	logbs.Infow("bitswap Get", "cid", c)
	defer func() {
		if finalErr != nil {
			logbs.Errorw("bitswap Get: got error", "cid", c, "error", finalErr)
		}
	}()

	mhash := c.Hash()
	// do we have the block cached ?
	if val, ok := ro.blkCache.Get(mhash.String()); ok {
		logbs.Infow("bitswap Get: returning from block cache", "cid", c)
		return val.(blocks.Block), nil
	}

	// fetch all the shards containing the multihash
	shards, err := ro.d.ShardsContainingMultihash(mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shards containing the block: %w", err)
	}
	if len(shards) == 0 {
		return nil, errors.New("no shards contain the requested block")
	}

	// do we have a cached blockstore for a shard containing the required block ? If yes, serve the block from that shard
	for _, sk := range shards {
		// a valid cache hit here updates the priority of the shard's blockstore in the LRU cache.
		val, ok := ro.bsCache.Get(sk)
		if !ok {
			continue
		}

		rbs := val.(*accessorWithBlockstore).bs
		blk, err := rbs.Get(ctx, c)
		if err != nil {
			ro.bsCache.Remove(sk)
			continue
		}

		// add the block to the block cache
		logbs.Infow("bitswap Get: returning from block store cache", "cid", c)
		ro.blkCache.Add(mhash.String(), blk)
		return blk, nil
	}

	// ---- we don't have a cached blockstore for a shard that can serve the block -> let's build one.

	// select a valid shard that can serve the retrieval
	sk, err := ro.shardSelectF(c, shards)
	if err != nil {
		return nil, fmt.Errorf("failed to select a shard: %w", err)
	}

	// load blockstore for the given shard
	resch := make(chan ShardResult, 1)
	// TODO Optmize index deserialisation in memory  to reduce the memory footprint of the cache
	if err := ro.d.AcquireShard(ctx, sk, resch, AcquireOpts{}); err != nil {
		return nil, fmt.Errorf("failed to acquire shard %s: %w", sk, err)
	}
	var res ShardResult
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res = <-resch:
		if res.Error != nil {
			return nil, fmt.Errorf("failed to acquire shard %s: %w", sk, res.Error)
		}
	}

	sa := res.Accessor
	bs, err := sa.Blockstore()
	if err != nil {
		return nil, fmt.Errorf("failed top load read only blockstore for shard %s: %w", sk, err)
	}

	blk, err := bs.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// update lru caches
	ro.bsCache.Add(sk, &accessorWithBlockstore{sa, bs})
	ro.blkCache.Add(mhash.String(), blk)

	logbs.Infow("bitswap Get: returning after creating new blockstore", "cid", c)
	return blk, nil
}

func (ro *AllShardsReadBlockstore) Has(_ context.Context, c cid.Cid) (bool, error) {
	logbs.Infow("bitswap Has", "cid", c)

	shards, err := ro.d.ShardsContainingMultihash(c.Hash())
	if err != nil {
		logbs.Errorw("bitswap Has", "cid", c, "error", err)
		return false, fmt.Errorf("failed to fetch shards containing the multihash %w", err)
	}
	if len(shards) == 0 {
		logbs.Infow("bitswap Has: returning false no error", "cid", c)
		return false, nil
	}

	// if there is a shard we can serve the retrieval from, we have the requested cid.
	_, err = ro.shardSelectF(c, shards)
	if err != nil {
		logbs.Errorw("bitswap Has", "cid", c, "err", err)
		return false, fmt.Errorf("failed to select a shard: %w", err)
	}

	logbs.Infow("bitswap Has: returning true", "cid", c)
	return true, nil
}

func (ro *AllShardsReadBlockstore) HashOnRead(_ bool) {
	panic(errors.New("unsupported operation HashOnRead"))
}

// GetSize returns the CIDs mapped BlockSize
func (ro *AllShardsReadBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	logbs.Infow("bitswap GetSize", "cid", c)

	blk, err := ro.Get(ctx, c)
	if err != nil {
		logbs.Errorw("bitswap GetSize error", "cid", c, "error", err)
		return 0, fmt.Errorf("failed to get block: %w", err)
	}

	logbs.Infow("bitswap GetSize success", "cid", c)
	return len(blk.RawData()), nil
}
func (ro *AllShardsReadBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("unsupported operation DeleteBlock")
}
func (ro *AllShardsReadBlockstore) Put(context.Context, blocks.Block) error {
	return errors.New("unsupported operation Put")
}
func (ro *AllShardsReadBlockstore) PutMany(context.Context, []blocks.Block) error {
	return errors.New("unsupported operation PutMany")
}
func (ro *AllShardsReadBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("unsupported operation AllKeysChan")
}

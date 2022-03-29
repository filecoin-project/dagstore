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

var logbs = logging.Logger("dagstore-all-readblockstore")

var _ blockstore.Blockstore = (*AllShardsReadBlockstore)(nil)

// ShardSelectorF helps select a shard to fetch a cid from if the given cid is present in multiple shards.
type ShardSelectorF func(c cid.Cid, shards []shard.Key) (shard.Key, error)

type accessorWithBlockstore struct {
	sa *ShardAccessor
	bs ReadBlockstore
}

// AllShardsReadBlockstore is a read only blockstore over all cids across all shards in the dagstore.
type AllShardsReadBlockstore struct {
	d            *DAGStore
	shardSelectF ShardSelectorF

	// caches the blockstore for a given shard for shard read affinity i.e. further reads will likely be from the same shard.
	// shard key -> read only blockstore
	blockstoreCache *lru.Cache

	// caches the blocks themselves -> can be scaled by using a redis/memcache etc distributed cache.
	// multihash -> block
	blockCache *lru.Cache
}

func (d *DAGStore) AllShardsReadBlockstore(shardSelector ShardSelectorF, maxCacheSize int, maxBlocks int) (blockstore.Blockstore, error) {
	// instantiate the blockstore cache
	bslru, err := lru.NewWithEvict(maxCacheSize, func(_ interface{}, val interface{}) {
		// ensure we close the blockstore for a shard when it's evicted from the cache so dagstore can gc it.
		abs := val.(*accessorWithBlockstore)
		abs.sa.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for read only blockstores")
	}

	// instantiate the block cache
	blkLru, err := lru.New(maxBlocks)
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for blocks: %w", err)
	}

	return &AllShardsReadBlockstore{
		d:               d,
		shardSelectF:    shardSelector,
		blockstoreCache: bslru,
		blockCache:      blkLru,
	}, nil
}

func (ro *AllShardsReadBlockstore) Get(ctx context.Context, c cid.Cid) (b blocks.Block, finalErr error) {
	logbs.Debugw("bitswap Get called", "cid", c)
	defer func() {
		if finalErr != nil {
			logbs.Debugw("bitswap Get: got error", "cid", c, "error", finalErr)
		}
	}()

	mhash := c.Hash()
	// do we have the block cached ?
	if val, ok := ro.blockCache.Get(mhash.String()); ok {
		logbs.Debugw("bitswap Get: returning from block cache", "cid", c)
		return val.(blocks.Block), nil
	}

	// fetch all the shards containing the multihash
	shards, err := ro.d.ShardsContainingMultihash(ctx, mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shards containing the block: %w", err)
	}
	if len(shards) == 0 {
		return nil, errors.New("no shards contain the requested block")
	}

	// do we have a cached blockstore for a shard containing the required block ? If yes, serve the block from that blockstore
	for _, sk := range shards {
		// a valid cache hit here updates the priority of the shard's blockstore in the LRU cache.
		val, ok := ro.blockstoreCache.Get(sk)
		if !ok {
			continue
		}

		rbs := val.(*accessorWithBlockstore).bs
		blk, err := rbs.Get(ctx, c)
		if err != nil {
			ro.blockstoreCache.Remove(sk)
			continue
		}

		// add the block to the block cache
		logbs.Debugw("bitswap Get: returning from block store cache", "cid", c)
		ro.blockCache.Add(mhash.String(), blk)
		return blk, nil
	}

	// ---- we don't have a cached blockstore for a shard that can serve the block -> let's build one.

	// select a valid shard that can serve the retrieval
	sk, err := ro.shardSelectF(c, shards)
	if err != nil {
		return nil, fmt.Errorf("failed to select a shard: %w", err)
	}

	// load blockstore for the selected shard and try to serve the cid from that blockstore.
	resch := make(chan ShardResult, 1)
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
		return nil, fmt.Errorf("failed to load read only blockstore for shard %s: %w", sk, err)
	}

	blk, err := bs.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// update the block cache and the blockstore cache
	ro.blockstoreCache.Add(sk, &accessorWithBlockstore{sa, bs})
	ro.blockCache.Add(mhash.String(), blk)

	logbs.Debugw("bitswap Get: returning after creating new blockstore", "cid", c)
	return blk, nil
}

func (ro *AllShardsReadBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	logbs.Debugw("bitswap Has called", "cid", c)

	// if there is a shard that can serve the retrieval for the given cid, we have the requested cid
	// and has should return true.
	shards, err := ro.d.ShardsContainingMultihash(ctx, c.Hash())
	if err != nil {
		logbs.Debugw("bitswap Has error", "cid", c, "err", err)
		return false, fmt.Errorf("failed to fetch shards containing the multihash %w", err)
	}
	if len(shards) == 0 {
		logbs.Debugw("bitswap Has: returning false no error", "cid", c)
		return false, nil
	}

	_, err = ro.shardSelectF(c, shards)
	if err != nil {
		logbs.Debugw("bitswap Has error", "cid", c, "err", err)
		return false, fmt.Errorf("failed to select a shard: %w", err)
	}

	logbs.Debugw("bitswap Has: returning true", "cid", c)
	return true, nil
}

func (ro *AllShardsReadBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	logbs.Debugw("bitswap GetSize called", "cid", c)

	blk, err := ro.Get(ctx, c)
	if err != nil {
		logbs.Debugw("bitswap GetSize error", "cid", c, "err", err)
		return 0, fmt.Errorf("failed to get block: %w", err)
	}

	logbs.Debugw("bitswap GetSize success", "cid", c)
	return len(blk.RawData()), nil
}

// --- UNSUPPORTED BLOCKSTORE METHODS -------
func (ro *AllShardsReadBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("unsupported operation DeleteBlock")
}
func (ro *AllShardsReadBlockstore) HashOnRead(_ bool) {}
func (ro *AllShardsReadBlockstore) Put(context.Context, blocks.Block) error {
	return errors.New("unsupported operation Put")
}
func (ro *AllShardsReadBlockstore) PutMany(context.Context, []blocks.Block) error {
	return errors.New("unsupported operation PutMany")
}
func (ro *AllShardsReadBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("unsupported operation AllKeysChan")
}

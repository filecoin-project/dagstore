package indexbs

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/dagstore"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/dagstore/shard"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var logbs = logging.Logger("dagstore-all-readblockstore")

var ErrBlockNotFound = errors.New("block not found")

var _ blockstore.Blockstore = (*IndexBackedBlockstore)(nil)

// ShardSelectorF helps select a shard to fetch a cid from if the given cid is present in multiple shards.
type ShardSelectorF func(c cid.Cid, shards []shard.Key) (shard.Key, error)

type accessorWithBlockstore struct {
	sa *dagstore.ShardAccessor
	bs dagstore.ReadBlockstore
}

// IndexBackedBlockstore is a read only blockstore over all cids across all shards in the dagstore.
type IndexBackedBlockstore struct {
	d            *dagstore.DAGStore
	shardSelectF ShardSelectorF

	// caches the blockstore for a given shard for shard read affinity i.e. further reads will likely be from the same shard.
	// shard key -> read only blockstore
	blockstoreCache *lru.Cache
}

func NewIndexBackedBlockstore(d *dagstore.DAGStore, shardSelector ShardSelectorF, maxCacheSize int, maxBlocks int) (blockstore.Blockstore, error) {
	// instantiate the blockstore cache
	bslru, err := lru.NewWithEvict(maxCacheSize, func(_ interface{}, val interface{}) {
		// ensure we close the blockstore for a shard when it's evicted from the cache so dagstore can gc it.
		abs := val.(*accessorWithBlockstore)
		abs.sa.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for read only blockstores")
	}

	return &IndexBackedBlockstore{
		d:               d,
		shardSelectF:    shardSelector,
		blockstoreCache: bslru,
	}, nil
}

func (ro *IndexBackedBlockstore) Get(ctx context.Context, c cid.Cid) (b blocks.Block, finalErr error) {
	logbs.Debugw("Get called", "cid", c)
	defer func() {
		if finalErr != nil {
			logbs.Debugw("Get: got error", "cid", c, "error", finalErr)
		}
	}()

	mhash := c.Hash()

	// fetch all the shards containing the multihash
	shards, err := ro.d.ShardsContainingMultihash(ctx, mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shards containing the block: %w", err)
	}
	if len(shards) == 0 {
		return nil, ErrBlockNotFound
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
			// we know that the cid we want to lookup belongs to a shard with key `sk` and
			// so if we fail to get the corresponding block from the blockstore for that shards, something has gone wrong
			// and we should remove the blockstore for that shard from our cache.
			ro.blockstoreCache.Remove(sk)
			continue
		}

		// add the block to the block cache
		logbs.Debugw("Get: returning from block store cache", "cid", c)
		return blk, nil
	}

	// ---- we don't have a cached blockstore for a shard that can serve the block -> let's build one.

	// select a valid shard that can serve the retrieval
	sk, err := ro.shardSelectF(c, shards)
	if err != nil {
		return nil, ErrBlockNotFound
	}

	// load blockstore for the selected shard and try to serve the cid from that blockstore.
	resch := make(chan dagstore.ShardResult, 1)
	if err := ro.d.AcquireShard(ctx, sk, resch, dagstore.AcquireOpts{}); err != nil {
		return nil, fmt.Errorf("failed to acquire shard %s: %w", sk, err)
	}
	var res dagstore.ShardResult
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

	logbs.Debugw("Get: returning after creating new blockstore", "cid", c)
	return blk, nil
}

func (ro *IndexBackedBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	logbs.Debugw("Has called", "cid", c)

	// if there is a shard that can serve the retrieval for the given cid, we have the requested cid
	// and has should return true.
	shards, err := ro.d.ShardsContainingMultihash(ctx, c.Hash())
	if err != nil {
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, nil
	}
	if len(shards) == 0 {
		logbs.Debugw("Has: returning false no error", "cid", c)
		return false, nil
	}

	_, err = ro.shardSelectF(c, shards)
	if err != nil {
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, ErrBlockNotFound
	}

	logbs.Debugw("Has: returning true", "cid", c)
	return true, nil
}

func (ro *IndexBackedBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	logbs.Debugw("GetSize called", "cid", c)

	blk, err := ro.Get(ctx, c)
	if err != nil {
		logbs.Debugw("GetSize error", "cid", c, "err", err)
		return 0, fmt.Errorf("failed to get block: %w", err)
	}

	logbs.Debugw("GetSize success", "cid", c)
	return len(blk.RawData()), nil
}

// --- UNSUPPORTED BLOCKSTORE METHODS -------
func (ro *IndexBackedBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("unsupported operation DeleteBlock")
}
func (ro *IndexBackedBlockstore) HashOnRead(_ bool) {}
func (ro *IndexBackedBlockstore) Put(context.Context, blocks.Block) error {
	return errors.New("unsupported operation Put")
}
func (ro *IndexBackedBlockstore) PutMany(context.Context, []blocks.Block) error {
	return errors.New("unsupported operation PutMany")
}
func (ro *IndexBackedBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("unsupported operation AllKeysChan")
}

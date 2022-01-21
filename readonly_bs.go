package dagstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/dagstore/shard"
	lru "github.com/hashicorp/golang-lru"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var _ blockstore.Blockstore = (*AllShardsReadBlockstore)(nil)

type ShardSelectorF func(c cid.Cid, shards []shard.Key) (shard.Key, error)

type AllShardsReadBlockstore struct {
	d            *DAGStore
	shardSelectF ShardSelectorF

	bsCache *lru.ARCCache // thread-safe
}

func (d *DAGStore) AllShardsReadBlockstore(shardSelector ShardSelectorF, maxCacheSize int) (blockstore.Blockstore, error) {
	lru, err := lru.NewARC(maxCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for read only blockstores")
	}

	return &AllShardsReadBlockstore{
		d:            d,
		shardSelectF: shardSelector,
		bsCache:      lru,
	}, nil

}

func (ro *AllShardsReadBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) { // get all the shards containing the mh
	shards, err := ro.d.ShardsContainingMultihash(c.Hash())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shards containing the block: %w", err)
	}

	// do we have a cached blockstore for a shard containing the given cid ? If yes, serve the block from that cid
	for _, sk := range shards {
		// a valid cache hit here updates the priority of the shard's blockstore in the LRU cache.
		val, ok := ro.bsCache.Get(sk)
		if !ok {
			continue
		}

		rbs := val.(ReadBlockstore)
		blk, err := rbs.Get(ctx, c)
		if err != nil {
			ro.bsCache.Remove(sk)
			continue
		}
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

	bs, err := res.Accessor.Blockstore()
	if err != nil {
		return nil, fmt.Errorf("failed top load read only blockstore for shard %s: %w", sk, err)
	}

	// update lru cache
	ro.bsCache.Add(sk, bs)

	return bs.Get(ctx, c)
}

func (ro *AllShardsReadBlockstore) Has(_ context.Context, c cid.Cid) (bool, error) {
	shards, err := ro.d.ShardsContainingMultihash(c.Hash())
	if err != nil {
		return false, fmt.Errorf("failed to fetch shards containing the multihash %w", err)
	}

	// if there is a shard we can serve the retrieval from, we have the requested cid.
	_, err = ro.shardSelectF(c, shards)
	if err != nil {
		return false, fmt.Errorf("failed to select a shard: %w", err)
	}

	return true, nil
}

func (ro *AllShardsReadBlockstore) HashOnRead(_ bool) {
	panic(errors.New("unsupported operation HashOnRead"))
}

// GetSize returns the CIDs mapped BlockSize
func (ro *AllShardsReadBlockstore) GetSize(context.Context, cid.Cid) (int, error) {
	return 100000000000, nil
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

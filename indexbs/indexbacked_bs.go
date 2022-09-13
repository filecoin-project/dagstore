package indexbs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	lru "github.com/hashicorp/golang-lru"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
)

var logbs = logging.Logger("dagstore/idxbs")

var ErrBlockNotFound = errors.New("block not found")

var _ blockstore.Blockstore = (*IndexBackedBlockstore)(nil)

// ErrNoShardSelected means that the shard selection function rejected all of the given shards.
var ErrNoShardSelected = errors.New("no shard selected")

// ShardSelectorF helps select a shard to fetch a cid from if the given cid is present in multiple shards.
// It should return `ErrNoShardSelected` if none of the given shard is selected.
type ShardSelectorF func(c cid.Cid, shards []shard.Key) (shard.Key, error)

// IndexBackedBlockstore is a read only blockstore over all cids across all shards in the dagstore.
type IndexBackedBlockstore struct {
	d            dagstore.Interface
	shardSelectF ShardSelectorF

	// caches the blockstore for a given shard for shard read affinity
	// i.e. further reads will likely be from the same shard. Maps (shard key -> blockstore).
	bsCache *blockstoreCache
}

func NewIndexBackedBlockstore(d dagstore.Interface, shardSelector ShardSelectorF, maxCacheSize int) (blockstore.Blockstore, error) {
	cache, err := newBlockstoreCache(maxCacheSize)
	if err != nil {
		return nil, err
	}

	return &IndexBackedBlockstore{
		d:            d,
		shardSelectF: shardSelector,
		bsCache:      cache,
	}, nil
}

type BlockstoreOp bool

const (
	BlockstoreOpGet     = true
	BlockstoreOpGetSize = !BlockstoreOpGet
)

func (o BlockstoreOp) String() string {
	if o == BlockstoreOpGet {
		return "Get"
	}
	return "GetSize"
}

type opRes struct {
	block blocks.Block
	size  int
}

func (ro *IndexBackedBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	res, err := ro.execOpWithLogs(ctx, c, BlockstoreOpGet)
	if err != nil {
		return nil, err
	}
	return res.block, err
}

func (ro *IndexBackedBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	res, err := ro.execOpWithLogs(ctx, c, BlockstoreOpGetSize)
	if err != nil {
		return 0, err
	}
	return res.size, err
}

func (ro *IndexBackedBlockstore) execOpWithLogs(ctx context.Context, c cid.Cid, op BlockstoreOp) (*opRes, error) {
	logbs.Debugw(op.String(), "cid", c)

	res, err := ro.execOp(ctx, c, op)
	if err != nil {
		logbs.Debugw(op.String()+" error", "cid", c, "error", err)
	} else {
		logbs.Debugw(op.String()+" success", "cid", c)
	}
	return res, err
}

func (ro *IndexBackedBlockstore) execOp(ctx context.Context, c cid.Cid, op BlockstoreOp) (*opRes, error) {
	// Fetch all the shards containing the multihash
	shards, err := ro.d.ShardsContainingMultihash(ctx, c.Hash())
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, ErrBlockNotFound
		}
		return nil, fmt.Errorf("failed to fetch shards containing block %s (multihash %s): %w", c, c.Hash(), err)
	}
	if len(shards) == 0 {
		// If there are no shards containing the multihash, return "not found"
		return nil, ErrBlockNotFound
	}

	// Do we have a cached blockstore for a shard containing the required block?
	// If so, call op on the cached blockstore.
	for _, sk := range shards {
		// Get the shard's blockstore from the cache
		abs, ok := ro.bsCache.Get(sk)
		if ok {
			res, err := execOpOnBlockstore(ctx, c, sk, abs.bs, op)
			abs.close()
			if err == nil {
				// Found a cached shard blockstore containing the required block,
				// and successfully called the blockstore op
				return res, nil
			}
		}
	}

	// We weren't able to get the block which means that either
	// 1. There is no cached blockstore for a shard that contains the block
	// 2. There was an error trying to get the block from the existing cached
	//    blockstore.
	//    ShardsContainingMultihash indicated that the shard has the block, so
	//    if there was an error getting it, it means there is something wrong.
	// So in either case we should create a new blockstore for the shard.

	// Use the shard select function to select one of the shards with the block
	sk, err := ro.shardSelectF(c, shards)
	if err != nil && errors.Is(err, ErrNoShardSelected) {
		// If none of the shards passes the selection filter, return "not found"
		return nil, ErrBlockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to run shard selection function: %w", err)
	}

	// Load the blockstore for the selected shard.
	// Note that internally the DAG store will synchronize multiple concurrent
	// acquires for the same shard.
	resch := make(chan dagstore.ShardResult, 1)
	if err := ro.d.AcquireShard(ctx, sk, resch, dagstore.AcquireOpts{}); err != nil {
		return nil, fmt.Errorf("failed to acquire shard %s: %w", sk, err)
	}
	var shres dagstore.ShardResult
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case shres = <-resch:
		if shres.Error != nil {
			return nil, fmt.Errorf("failed to acquire shard %s: %w", sk, shres.Error)
		}
	}

	sa := shres.Accessor
	bs, err := sa.Blockstore()
	if err != nil {
		return nil, fmt.Errorf("failed to load read-only blockstore for shard %s: %w", sk, err)
	}

	// Add the blockstore to the cache
	abs := &accessorWithBlockstore{sa: sa, bs: bs}
	ro.bsCache.Add(sk, abs)
	defer abs.close()

	logbs.Debugw("Added new blockstore to cache", "cid", c, "shard", sk)

	// Call the operation on the blockstore
	return execOpOnBlockstore(ctx, c, sk, bs, op)
}

func execOpOnBlockstore(ctx context.Context, c cid.Cid, sk shard.Key, bs dagstore.ReadBlockstore, op BlockstoreOp) (*opRes, error) {
	var err error
	var res opRes
	switch op {
	case BlockstoreOpGet:
		res.block, err = bs.Get(ctx, c)
	case BlockstoreOpGetSize:
		res.size, err = bs.GetSize(ctx, c)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to call blockstore.%s for shard %s: %w", op, sk, err)
	}
	return &res, nil
}

func (ro *IndexBackedBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	logbs.Debugw("Has", "cid", c)

	// Get shards that contain the cid's hash
	shards, err := ro.d.ShardsContainingMultihash(ctx, c.Hash())
	if err != nil {
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, nil
	}
	if len(shards) == 0 {
		logbs.Debugw("Has: returning false", "cid", c)
		return false, nil
	}

	// Check if there is a shard with the block that is not filtered out by
	// the shard selection function
	_, err = ro.shardSelectF(c, shards)
	if err != nil && errors.Is(err, ErrNoShardSelected) {
		logbs.Debugw("Has: returning false", "cid", c)
		return false, nil
	}
	if err != nil {
		err = fmt.Errorf("failed to run shard selection function: %w", err)
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, err
	}

	logbs.Debugw("Has: returning true", "cid", c)
	return true, nil
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

type blockstoreCache struct {
	lk    sync.Mutex
	cache *lru.Cache
}

func newBlockstoreCache(size int) (*blockstoreCache, error) {
	bslru, err := lru.NewWithEvict(size, func(_ interface{}, val interface{}) {
		abs := val.(*accessorWithBlockstore)
		abs.evict()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for read only blockstores: %w", err)
	}

	return &blockstoreCache{cache: bslru}, nil
}

func (bc *blockstoreCache) Get(sk shard.Key) (*accessorWithBlockstore, bool) {
	bc.lk.Lock()
	defer bc.lk.Unlock()

	// Get the accessor from the cache
	absi, ok := bc.cache.Get(sk)
	if !ok {
		return nil, false
	}

	// Increment the accessor's ref count so that the blockstore
	// will not be closed until the caller is finished with it
	abs := absi.(*accessorWithBlockstore)
	abs.incRefCount()
	return abs, true
}

func (bc *blockstoreCache) Add(sk shard.Key, abs *accessorWithBlockstore) {
	bc.lk.Lock()
	defer bc.lk.Unlock()

	// Check if we're replacing an existing accessor with this Add
	absi, ok := bc.cache.Get(sk)
	if ok {
		// Mark the existing accessor as evicted so that its blockstore can be
		// closed once all callers are done with the blockstore
		abs := absi.(*accessorWithBlockstore)
		abs.evict()
	}

	// Add the new accessor
	bc.cache.Add(sk, abs)
	abs.incRefCount()
}

type accessorWithBlockstore struct {
	sa *dagstore.ShardAccessor
	bs dagstore.ReadBlockstore

	lk       sync.Mutex
	evicted  bool
	refCount int
}

func (abs *accessorWithBlockstore) incRefCount() {
	abs.lk.Lock()
	defer abs.lk.Unlock()

	abs.refCount++
}

func (abs *accessorWithBlockstore) close() {
	abs.lk.Lock()
	defer abs.lk.Unlock()

	abs.refCount--
	if abs.refCount == 0 && abs.evicted {
		// The blockstore has already been evicted, and this was the last
		// reference to it, so close the blockstore so that dagstore can GC it
		err := abs.sa.Close()
		if err != nil {
			logbs.Warnf("error closing blockstore: %w", err)
		}
	}
}

func (abs *accessorWithBlockstore) evict() {
	abs.lk.Lock()
	defer abs.lk.Unlock()

	abs.evicted = true

	if abs.refCount == 0 {
		// There are no more references to the blockstore; close it so that the
		// dagstore can GC it
		err := abs.sa.Close()
		if err != nil {
			logbs.Warnf("error closing blockstore: %w", err)
		}
	}
}

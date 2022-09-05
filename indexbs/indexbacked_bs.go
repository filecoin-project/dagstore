package indexbs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	lru "github.com/hnlq715/golang-lru"
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

type accessorWithBlockstore struct {
	sa *dagstore.ShardAccessor
	bs dagstore.ReadBlockstore
}

// IndexBackedBlockstore is a read only blockstore over all cids across all shards in the dagstore.
type IndexBackedBlockstore struct {
	d            dagstore.Interface
	shardSelectF ShardSelectorF

	bsStripedLocks [256]sync.Mutex
	// caches the blockstore for a given shard for shard read affinity
	// i.e. further reads will likely be from the same shard. Maps (shard key -> blockstore).
	blockstoreCache *lru.Cache
}

func NewIndexBackedBlockstore(d dagstore.Interface, shardSelector ShardSelectorF, maxCacheSize int) (blockstore.Blockstore, error) {
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
		// Use a striped lock to synchronize between this code that gets from
		// the cache and the code below that adds to the cache
		lk := &ro.bsStripedLocks[shardKeyToStriped(sk)]
		lk.Lock()
		res, err := ro.readFromBSCacheUnlocked(ctx, c, sk, op)
		lk.Unlock()
		if err == nil {
			// Found a cached shard blockstore containing the required block,
			// and successfully called the blockstore op
			return res, nil
		}
	}

	// We don't have a cached blockstore for a shard that contains the block.
	// Let's build one.

	// Use the shard select function to select one of the shards with the block
	sk, err := ro.shardSelectF(c, shards)
	if err != nil && errors.Is(err, ErrNoShardSelected) {
		// If none of the shards passes the selection filter, return "not found"
		return nil, ErrBlockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to run shard selection function: %w", err)
	}

	// Synchronize between the code above that gets a blockstore from the cache
	// and the code below that adds a blockstore to the cache
	lk := &ro.bsStripedLocks[shardKeyToStriped(sk)]
	lk.Lock()
	defer lk.Unlock()

	// Check if another thread already added the shard's blockstore to the
	// cache while this thread was waiting to obtain the lock
	res, err := ro.readFromBSCacheUnlocked(ctx, c, sk, op)
	if err == nil {
		return res, nil
	}

	// Load the blockstore for the selected shard
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

	// Call the operation on the blockstore
	res, err = execOpOnBlockstore(ctx, c, sk, bs, op)
	if err != nil {
		return nil, err
	}

	// Update the blockstore cache
	ro.blockstoreCache.Add(sk, &accessorWithBlockstore{sa, bs})

	logbs.Debugw("Added new blockstore to cache", "cid", c, "shard", sk)
	return res, nil
}

func (ro *IndexBackedBlockstore) readFromBSCacheUnlocked(ctx context.Context, c cid.Cid, shardContainingCid shard.Key, op BlockstoreOp) (*opRes, error) {
	// Get the shard's blockstore from the cache
	val, ok := ro.blockstoreCache.Get(shardContainingCid)
	if !ok {
		return nil, ErrBlockNotFound
	}

	accessor := val.(*accessorWithBlockstore)
	res, err := execOpOnBlockstore(ctx, c, shardContainingCid, accessor.bs, op)
	if err == nil {
		return res, nil
	}

	// We know that the cid we want to lookup belongs to a shard with key `sk` and
	// so if we fail to get the corresponding block from the blockstore for that shard,
	// something has gone wrong and we should remove the blockstore for that shard from our cache.
	// However there may be several calls from different threads waiting to acquire
	// the blockstore from the cache, so to prevent flapping, set a short expiry on the
	// cache key instead of removing it immediately.
	logbs.Warnf("expected blockstore for shard %s to contain cid %s (multihash %s) but it did not",
		shardContainingCid, c, c.Hash())
	ro.blockstoreCache.AddEx(shardContainingCid, accessor, time.Second)
	return nil, err
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

func shardKeyToStriped(sk shard.Key) byte {
	// The shard key is typically a cid, so the last byte should be random.
	// Use the last byte as as the striped lock index.
	return sk.String()[len(sk.String())-1]
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

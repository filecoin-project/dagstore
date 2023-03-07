package indexbs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/jellydator/ttlcache/v2"
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

type IdxBstoreDagstore interface {
	ShardsContainingCid(ctx context.Context, c cid.Cid) ([]shard.Key, error)
	AcquireShard(ctx context.Context, key shard.Key, out chan dagstore.ShardResult, _ dagstore.AcquireOpts) error
}

// IndexBackedBlockstore is a read only blockstore over all cids across all shards in the dagstore.
type IndexBackedBlockstore struct {
	ctx          context.Context
	d            IdxBstoreDagstore
	shardSelectF ShardSelectorF

	// caches the blockstore for a given shard for shard read affinity
	// i.e. further reads will likely be from the same shard. Maps (shard key -> blockstore).
	blockstoreCache *ttlcache.Cache
	// used to manage concurrent acquisition of shards by multiple threads
	stripedLock [256]sync.Mutex
}

func NewIndexBackedBlockstore(ctx context.Context, d IdxBstoreDagstore, shardSelector ShardSelectorF, maxCacheSize int, cacheExpire time.Duration) (blockstore.Blockstore, error) {
	cache := ttlcache.NewCache()
	cache.SetTTL(cacheExpire)
	cache.SetCacheSizeLimit(maxCacheSize)
	cache.SetExpirationReasonCallback(func(_ string, _ ttlcache.EvictionReason, val interface{}) {
		// Ensure we close the blockstore for a shard when it's evicted from
		// the cache so dagstore can gc it.
		// TODO: add reference counting mechanism so that the blockstore does
		// not get closed while there is an operation still in progress against it
		abs := val.(*accessorWithBlockstore)
		abs.sa.Close()
	})

	return &IndexBackedBlockstore{
		ctx:             ctx,
		d:               d,
		shardSelectF:    shardSelector,
		blockstoreCache: cache,
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
	shards, err := ro.d.ShardsContainingCid(ctx, c)
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
		val, err := ro.blockstoreCache.Get(sk.String())
		if err != nil {
			continue
		}

		if val != nil {
			accessor := val.(*accessorWithBlockstore)
			res, err := execOpOnBlockstore(ctx, c, sk, accessor.bs, op)
			if err != nil {
				return nil, err
			}
			return res, nil
		}
	}

	// We weren't able to find a cached blockstore for a shard that contains
	// the block. Create a new blockstore for the shard.

	// Use the shard select function to select one of the shards with the block
	sk, err := ro.shardSelectF(c, shards)
	if err != nil && errors.Is(err, ErrNoShardSelected) {
		// If none of the shards passes the selection filter, return "not found"
		return nil, ErrBlockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to run shard selection function: %w", err)
	}

	// Some retrieval patterns will result in multiple threads fetching blocks
	// from the same piece concurrently. In that case many threads may attempt
	// to create a blockstore over the same piece. Use a striped lock to ensure
	// that the blockstore is only created once for all threads waiting on the
	// same shard.
	bs, err := func() (dagstore.ReadBlockstore, error) {
		// Derive the striped lock index from the shard key and acquire the lock
		skstr := sk.String()
		lockIdx := skstr[len(skstr)-1]
		ro.stripedLock[lockIdx].Lock()
		defer ro.stripedLock[lockIdx].Unlock()

		// Check if the blockstore was created by another thread while this
		// thread was waiting to enter the lock
		val, err := ro.blockstoreCache.Get(sk.String())
		if err == nil && val != nil {
			return val.(*accessorWithBlockstore).bs, nil
		}

		// Acquire the blockstore for the selected shard
		resch := make(chan dagstore.ShardResult, 1)
		if err := ro.d.AcquireShard(ro.ctx, sk, resch, dagstore.AcquireOpts{}); err != nil {
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
		ro.blockstoreCache.Set(sk.String(), &accessorWithBlockstore{sa, bs})

		logbs.Debugw("Added new blockstore to cache", "cid", c, "shard", sk)

		return bs, nil
	}()
	if err != nil {
		return nil, err
	}

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
	shards, err := ro.d.ShardsContainingCid(ctx, c)
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

type IdxBstoreDagstoreFromDagstore struct {
	dagstore.Interface
}

var _ IdxBstoreDagstore = (*IdxBstoreDagstoreFromDagstore)(nil)

func (d *IdxBstoreDagstoreFromDagstore) ShardsContainingCid(ctx context.Context, c cid.Cid) ([]shard.Key, error) {
	return d.Interface.ShardsContainingMultihash(ctx, c.Hash())
}

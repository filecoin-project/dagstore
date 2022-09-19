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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

type blockstoreAcquire struct {
	once sync.Once
	bs   dagstore.ReadBlockstore
	err  error
}

// IndexBackedBlockstore is a read only blockstore over all cids across all shards in the dagstore.
type IndexBackedBlockstore struct {
	d            dagstore.Interface
	shardSelectF ShardSelectorF

	// caches the blockstore for a given shard for shard read affinity
	// i.e. further reads will likely be from the same shard. Maps (shard key -> blockstore).
	blockstoreCache *lru.Cache
	// used to manage concurrent acquisition of shards by multiple threads
	bsAcquireByShard sync.Map

	tracer trace.Tracer
}

func NewIndexBackedBlockstore(d dagstore.Interface, shardSelector ShardSelectorF, maxCacheSize int, t trace.Tracer) (blockstore.Blockstore, error) {
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
		tracer:          t,
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
	var span trace.Span
	if ro.tracer != nil {
		ctx, span = ro.tracer.Start(ctx, "ibb.execOp")
		defer span.End()
		span.SetAttributes(attribute.String("cid", c.String()))
		span.SetAttributes(attribute.String("op", op.String()))
	}

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
		val, ok := ro.blockstoreCache.Get(sk)
		if ok {
			var span2 trace.Span
			if ro.tracer != nil {
				ctx, span2 = ro.tracer.Start(ctx, "ibb.execOpOnBlockstore")
			}

			accessor := val.(*accessorWithBlockstore)
			res, err := execOpOnBlockstore(ctx, c, sk, accessor.bs, op)
			if err != nil {

				span2.SetAttributes(attribute.String("err", err.Error()))
				span2.End()
				return nil, err
			}

			span2.End()

			// Found a cached blockstore containing the required block,
			// and successfully called the blockstore op
			return res, nil
		}
	}

	// We weren't able to find a cached blockstore for a shard that contains
	// the block. Create a new blockstore for the shard.

	var span3 trace.Span
	if ro.tracer != nil {
		ctx, span3 = ro.tracer.Start(ctx, "ibb.shardSelect")
	}

	// Use the shard select function to select one of the shards with the block
	sk, err := ro.shardSelectF(c, shards)
	if err != nil && errors.Is(err, ErrNoShardSelected) {

		span3.SetAttributes(attribute.String("err", err.Error()))
		span3.End()
		// If none of the shards passes the selection filter, return "not found"
		return nil, ErrBlockNotFound
	}
	if err != nil {
		span3.SetAttributes(attribute.String("err", err.Error()))
		span3.End()
		return nil, fmt.Errorf("failed to run shard selection function: %w", err)
	}

	span3.End()

	// Some retrieval patterns will result in multiple threads fetching blocks
	// from the same piece concurrently. In that case many threads may attempt
	// to create a blockstore over the same piece. Use a sync.Once to ensure
	// that the blockstore is only created once for all threads waiting on the
	// same shard.

	var span4 trace.Span
	if ro.tracer != nil {
		ctx, span4 = ro.tracer.Start(ctx, "ibb.bsAcquire")
	}

	bsAcquireI, _ := ro.bsAcquireByShard.LoadOrStore(sk, &blockstoreAcquire{})
	bsAcquire := bsAcquireI.(*blockstoreAcquire)
	bsAcquire.once.Do(func() {

		var span5 trace.Span
		if ro.tracer != nil {
			ctx, span5 = ro.tracer.Start(ctx, "ibb.bsAcquire.once")
		}
		defer span5.End()

		bsAcquire.bs, bsAcquire.err = func() (dagstore.ReadBlockstore, error) {
			// Check if the blockstore was created by another thread while this
			// thread was waiting to enter the sync.Once
			val, ok := ro.blockstoreCache.Get(sk)
			if ok {
				return val.(dagstore.ReadBlockstore), nil
			}

			// Acquire the blockstore for the selected shard
			resch := make(chan dagstore.ShardResult, 1)
			if err := ro.d.AcquireShard(context.Background(), sk, resch, dagstore.AcquireOpts{}); err != nil {
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
			ro.blockstoreCache.Add(sk, &accessorWithBlockstore{sa, bs})

			logbs.Debugw("Added new blockstore to cache", "cid", c, "shard", sk)

			return bs, nil
		}()

		// The sync.Once has completed so clean up the acquire entry for this shard
		ro.bsAcquireByShard.Delete(sk)
	})

	if bsAcquire.err != nil {
		span4.SetAttributes(attribute.String("err", bsAcquire.err.Error()))
		span4.End()
		return nil, bsAcquire.err
	}

	// Call the operation on the blockstore
	res, err := execOpOnBlockstore(ctx, c, sk, bsAcquire.bs, op)
	if err != nil {
		span4.SetAttributes(attribute.String("err", err.Error()))
		span4.End()
		return nil, err
	}

	span4.End()

	return res, nil
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

package dagstore

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"

	"golang.org/x/exp/mmap"
)

// ReadBlockstore is a read-only view of Blockstores. This will be implemented
// by the CARv2 indexed blockstore.
type ReadBlockstore interface {
	Has(context.Context, cid.Cid) (bool, error)
	Get(context.Context, cid.Cid) (blocks.Block, error)
	GetSize(context.Context, cid.Cid) (int, error)
	AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)
	HashOnRead(enabled bool)
}

// ShardAccessor provides various means to access the data contained
// in a shard.
type ShardAccessor struct {
	data  mount.Reader
	idx   index.Index
	shard *Shard

	// mmapr is an optional mmap.ReaderAt. It will be non-nil if the mount
	// has been mmapped because the mount.Reader was an underlying *os.File,
	// and an mmap-backed accessor was requested (e.g. Blockstore).
	lk    sync.Mutex
	mmapr *mmap.ReaderAt
}

func NewShardAccessor(data mount.Reader, idx index.Index, s *Shard) (*ShardAccessor, error) {
	return &ShardAccessor{
		data:  data,
		idx:   idx,
		shard: s,
	}, nil
}

func (sa *ShardAccessor) Shard() shard.Key {
	return sa.shard.key
}

func (sa *ShardAccessor) Read(p []byte) (int, error) {
	return sa.data.Read(p)
}

func (sa *ShardAccessor) Blockstore() (ReadBlockstore, error) {
	var r io.ReaderAt = sa.data

	sa.lk.Lock()
	if f, ok := sa.data.(*os.File); ok {
		if mmapr, err := mmap.Open(f.Name()); err != nil {
			log.Warnf("failed to mmap reader of type %T: %s; using reader as-is", sa.data, err)
		} else {
			// we don't close the mount.Reader file descriptor because the user
			// may have called other non-mmap-backed accessors.
			r = mmapr
			sa.mmapr = mmapr
		}
	}
	sa.lk.Unlock()

	bs, err := blockstore.NewReadOnly(r, sa.idx, carv2.ZeroLengthSectionAsEOF(true))
	return bs, err
}

// Close terminates this shard accessor, releasing any resources associated
// with it, and decrementing internal refcounts.
func (sa *ShardAccessor) Close() error {
	if err := sa.data.Close(); err != nil {
		log.Warnf("failed to close mount when closing shard accessor: %s", err)
	}
	sa.lk.Lock()
	if sa.mmapr != nil {
		if err := sa.mmapr.Close(); err != nil {
			log.Warnf("failed to close mmap when closing shard accessor: %s", err)
		}
	}
	sa.lk.Unlock()

	tsk := &task{op: OpShardRelease, shard: sa.shard}
	return sa.shard.d.queueTask(tsk, sa.shard.d.externalCh)
}

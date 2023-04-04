package dagstore

import (
	"context"
	"fmt"
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

// Reader returns an io.Reader that can be used to read the data from the shard.
func (sa *ShardAccessor) Reader() io.Reader {
	return &readerAtWrapper{
		readerAt: sa.tryMmap(),
	}
}

func (sa *ShardAccessor) Blockstore() (ReadBlockstore, error) {
	r := sa.tryMmap()
	bs, err := blockstore.NewReadOnly(r, sa.idx, carv2.ZeroLengthSectionAsEOF(true))
	return bs, err
}

// tryMmap attempts to mmap the file if the underlying data is an *os.File. It returns an
// io.ReaderAt which can be used to read the data. If the operation was successful or the file is
// already mapped , it will return the mmap.ReaderAt. If the memory mapping fails, it falls back to
// the original io.ReaderAt implementation from the mount.Reader and logs a warning message.
// The method is safe for concurrent use.
func (sa *ShardAccessor) tryMmap() io.ReaderAt {
	sa.lk.Lock()
	defer sa.lk.Unlock()

	if sa.mmapr != nil {
		return sa.mmapr
	}

	if f, ok := sa.data.(*os.File); ok {
		if mmapr, err := mmap.Open(f.Name()); err != nil {
			log.Warnf("failed to mmap reader of type %T: %s; using reader as-is", sa.data, err)
		} else {
			// we don't close the mount.Reader file descriptor because the user
			// may have called other non-mmap-backed accessors.
			sa.mmapr = mmapr
			return mmapr
		}
	}

	return sa.data
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

// readerAtWrapper is a wrapper around an io.ReaderAt that implements io.Reader.
type readerAtWrapper struct {
	readerAt   io.ReaderAt
	readOffset int64
}

func (w *readerAtWrapper) Read(p []byte) (n int, err error) {
	n, err = w.readerAt.ReadAt(p, w.readOffset)
	w.readOffset += int64(n)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("readerAtWrapper: error reading from the underlying ReaderAt: %w", err)
	}

	return n, err
}

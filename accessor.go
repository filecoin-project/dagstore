package dagstore

import (
	"context"
	"fmt"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"io"

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
		readerAt: sa.data,
	}
}

func (sa *ShardAccessor) Blockstore() (ReadBlockstore, error) {
	bs, err := blockstore.NewReadOnly(sa.data, sa.idx, carv2.ZeroLengthSectionAsEOF(true))
	return bs, err
}

// Close terminates this shard accessor, releasing any resources associated
// with it, and decrementing internal refcounts.
func (sa *ShardAccessor) Close() error {
	if err := sa.data.Close(); err != nil {
		log.Warnf("failed to close mount when closing shard accessor: %s", err)
	}

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

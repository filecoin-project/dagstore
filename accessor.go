package dagstore

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
)

// ReadBlockstore is a read-only view of Blockstores. This will be implemented
// by the CARv2 indexed blockstore.
type ReadBlockstore interface {
	Has(cid.Cid) (bool, error)
	Get(cid.Cid) (blocks.Block, error)
	GetSize(cid.Cid) (int, error)
	AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)
	HashOnRead(enabled bool)
}

// ShardAccessor provides various means to access the data contained
// in a shard.
type ShardAccessor struct {
	key   shard.Key
	data  mount.Reader
	idx   index.Index
	shard *Shard
}

func NewShardAccessor(key shard.Key, data mount.Reader, idx index.Index, s *Shard) (*ShardAccessor, error) {
	return &ShardAccessor{
		key:   key,
		data:  data,
		idx:   idx,
		shard: s,
	}, nil
}

func (sa *ShardAccessor) Shard() shard.Key {
	return sa.key
}

func (sa *ShardAccessor) Blockstore() (ReadBlockstore, error) {
	bs, err := blockstore.NewReadOnly(sa.data, sa.idx)
	return bs, err
}

// Close terminates this shard accessor, releasing any resources associated
// with it, and decrementing internal refcounts.
func (sa *ShardAccessor) Close() error {
	if err := sa.data.Close(); err != nil {
		return fmt.Errorf("failed to close shard: %w", err)
	}
	tsk := &task{op: OpShardRelease, shard: sa.shard}
	return sa.shard.d.queueTask(tsk, sa.shard.d.externalCh)
}

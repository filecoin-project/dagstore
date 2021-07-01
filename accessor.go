package dagstore

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
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
	key  shard.Key
	data *car.Reader
	idx  index.Index
}

func NewShardAccessor(key shard.Key, data mount.Reader, idx index.Index) (*ShardAccessor, error) {
	reader, err := car.NewReader(data)
	if err != nil {
		return nil, fmt.Errorf("failed to determine car version when opening shard accessor: %w", err)
	}

	return &ShardAccessor{
		key:  key,
		data: reader,
		idx:  idx,
	}, nil
}

func (sa *ShardAccessor) Shard() shard.Key {
	return sa.key
}

func (sa *ShardAccessor) Blockstore() (ReadBlockstore, error) {
	bs := blockstore.ReadOnlyOf(sa.data.CarV1Reader(), sa.idx)
	return bs, nil
}

// Close terminates this shard accessor, releasing any resources associated
// with it, and decrementing internal refcounts.
func (sa *ShardAccessor) Close() error {
	return sa.data.Close()
}

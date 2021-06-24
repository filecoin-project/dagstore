package dagstore

import (
	"context"

	"github.com/filecoin-project/dagstore/shard"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
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
type ShardAccessor interface {
	Shard() shard.Key
	Blockstore() (ReadBlockstore, error)
}

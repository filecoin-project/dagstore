package dagstore

import (
	"context"
	"math/rand"

	"github.com/filecoin-project/dagstore/mount"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
)

// by the CARv2 indexed blockstore.
type MultiBlockstore struct {
	index index.Index
	bs    []ReadBlockstore
}

func NewMultiBlockstore(readers []mount.Reader, i index.Index) (*MultiBlockstore, error) {
	mbs := &MultiBlockstore{index: i}

	for _, r := range readers {
		bs, err := blockstore.NewReadOnly(r, i, carv2.ZeroLengthSectionAsEOF(true))
		if err != nil {
			return nil, err
		}

		mbs.bs = append(mbs.bs, bs)
	}

	return mbs, nil
}

func (ms *MultiBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return ms.bs[0].Has(ctx, c)
}

func (ms *MultiBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	i := rand.Intn(len(ms.bs))

	return ms.bs[i].Get(ctx, c)
}

func (ms *MultiBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	i := rand.Intn(len(ms.bs))

	return ms.bs[i].GetSize(ctx, c)
}

func (ms *MultiBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return ms.bs[0].AllKeysChan(ctx)
}

func (ms *MultiBlockstore) HashOnRead(enabled bool) {
	ms.bs[0].HashOnRead(enabled)
}

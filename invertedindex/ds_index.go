package invertedindex

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/dagstore/shard"
)

var log = logging.Logger("dagstore.invidx")

var _ Index = (*DataStoreIndex)(nil)

const prefix = "/dagstore/inverted"

type DataStoreIndex struct {
	ds ds.Batching
}

func NewDataStoreIndex(d ds.Batching) *DataStoreIndex {
	log.Debugf("Creating new datastore cid -> shard index")
	wds := namespace.Wrap(d, ds.NewKey(prefix))

	return &DataStoreIndex{
		ds: wds,
	}
}

func (d *DataStoreIndex) AddCidsForShard(cidIter CidIterator, s shard.Key) error {
	batch, err := d.ds.Batch()
	if err != nil {
		return fmt.Errorf("failed to create ds batch: %w", err)
	}

	for _, c := range cidIter {
		cstr := c.String()
		ck := ds.NewKey(cstr)

		// do we already have an entry for the cid ?
		sbz, err := d.ds.Get(ck)

		if err != nil && err != ds.ErrNotFound {
			return fmt.Errorf("failed to get shard keys for cid=%s", c.String())
		}

		if err == ds.ErrNotFound {
			sarr := []shard.Key{s}
			bz, err := json.Marshal(sarr)
			if err != nil {
				return fmt.Errorf("failed to marshal shard list to bytes: %w", err)
			}
			if err := batch.Put(ck, bz); err != nil {
				return fmt.Errorf("failed to put cid=%s, err=%w", c.String(), err)
			}

			log.Debugf("+ %s -> %s", cstr, s)
			continue
		}

		var es []shard.Key
		if err := json.Unmarshal(sbz, &es); err != nil {
			return fmt.Errorf("failed to unmarshal shard keys: %w", err)
		}
		es = append(es, s)

		bz, err := json.Marshal(es)
		if err != nil {
			return fmt.Errorf("failed to marshal shard keys: %w", err)
		}
		if err := batch.Put(ck, bz); err != nil {
			return fmt.Errorf("failed to put cid=%s, err=%w", c.String(), err)
		}

		log.Debugf("+ %s -> %s", cstr, s)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("failed to commit ds batch, err=%w", err)
	}

	return nil
}

func (d *DataStoreIndex) DeleteCidsForShard(sk shard.Key, cidIterator CidIterator) error {
	batch, err := d.ds.Batch()
	if err != nil {
		return fmt.Errorf("failed to create ds batch: %w", err)
	}

	for _, c := range cidIterator {
		cstr := c.String()
		ck := ds.NewKey(cstr)

		sbz, err := d.ds.Get(ck)
		if err != nil {
			return fmt.Errorf("failed to get shards for cid=%s, err=%w", c, err)
		}
		var es []shard.Key
		if err := json.Unmarshal(sbz, &es); err != nil {
			return fmt.Errorf("failed to unmarshal shard keys: %w", err)
		}

		newShards := make([]shard.Key, 0, len(es)-1)
		for _, s := range es {
			if s != sk {
				newShards = append(newShards, s)
			}
		}

		sbz2, err := json.Marshal(newShards)
		if err != nil {
			return fmt.Errorf("failed to marshal shard keys: %w", err)
		}

		if err := batch.Put(ck, sbz2); err != nil {
			return fmt.Errorf("failed to put cid=%s, err=%w", c, err)
		}

		log.Debugf("- %s -> %s", cstr, sk)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("failed to commit ds batch, err=%w", err)
	}

	return nil
}

func (d *DataStoreIndex) GetShardsForCid(c cid.Cid) ([]shard.Key, error) {
	ck := ds.NewKey(c.String())
	sbz, err := d.ds.Get(ck)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index for cid %s, err: %w", c, err)
	}

	var shardKeys []shard.Key

	if err := json.Unmarshal(sbz, &shardKeys); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shard keys for cid=%s, err=%w", c, err)
	}

	return shardKeys, nil
}

func (d *DataStoreIndex) NCids() (uint64, error) {
	res, err := d.ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		return 0, err
	}

	entries, err := res.Rest()
	if err != nil {
		return 0, err
	}

	return uint64(len(entries)), err
}

func (d *DataStoreIndex) Iterator() (Iterator, error) {
	results, err := d.ds.Query(query.Query{})
	if err != nil {
		return nil, fmt.Errorf("failed to recover dagstore state from store: %w", err)
	}
	return &iteratorImpl{results}, nil
}

var _ Iterator = (*iteratorImpl)(nil)

type iteratorImpl struct {
	res query.Results
}

func (i *iteratorImpl) Next() (has bool, entry IndexEntry, err error) {
	res, has := i.res.NextSync()
	if !has {
		return has, IndexEntry{}, nil
	}

	c, err := cid.Decode(res.Key)
	if err != nil {
		return false, IndexEntry{}, fmt.Errorf("failed to decode cid=%s, err=%w", res.Key, err)
	}

	var shardKeys []shard.Key

	if err := json.Unmarshal(res.Value, &shardKeys); err != nil {
		return false, IndexEntry{}, fmt.Errorf("failed to unmarshal shard keys, err=%w", err)
	}

	return true, IndexEntry{
		Cid:    c,
		Shards: shardKeys,
	}, nil
}

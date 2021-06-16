package index

import (
	"io"

	"github.com/ipfs/go-cid"
	"github.com/willscott/carbs"
)

// FullShardIndex is the full Index of a sharded DAG over it's CARv1 payload. It provides a (Cid -> CARv1 payload offset) mapping
// for each Cid in the Sharded DAG where offset is relative to the beginning of the sharded DAG's CARv1 payload.
//
// In addition to providing random access to Cids/Nodes in the Sharded DAG, this Index is also useful as a full Index to enlist
// all the Cids present in the sharded DAG.
//
// TODO-BEGIN (for data-systems)
// 1. The Carbs Index needs to become iterable.
// 2. Carbs Index should have separate Read/Write Interfaces. The Index Store ONLY cares about the Read interface of the Index.
// 3. Should be able to create a Carbs Index from an io.Reader to allow index streaming from any source.
// 4. Replace dep on carbs Index with a go-car type once it's ported over to go-car.
// TODO-END
type FullShardIndex carbs.Index

// ReadOnlyShardIndex provides read-only access to the full shard Index.
// TODO: Change to read only concrete type once it's available.
type ReadOnlyShardIndex carbs.Index

// FullShardIndexFetcher allows a caller to stream a FullShardIndex for a sharded DAG.
// This abstraction enables the Index store to fetch and stream an Index from multiple sources.
// This abstraction simply returns a raw byte stream. It is the caller's responsibility to determine the Index type and unmarshal it.
// It is also the caller's responsibility to close the Index stream once it's done reading it.
type FullShardIndexFetcher interface {
	Fetch() (io.ReadCloser, error)
}

// FullShardIndexStore manages and allows access to the Full Indices built on top of the shards managed by the Sharded DAG Store.
type FullShardIndexStore interface {
	// AddFullShardIndex adds a full shard Index for the sharded DAG with the given shard key to the Store.
	// It will stream the raw Index byte stream using the given fetcher, unmarshal it to a full shard Index
	// using the given unmarshaler and then store and start tracking the Index.
	AddFullShardIndex(shard_key string, fetcher FullShardIndexFetcher, unmarshaler func(reader io.Reader) ReadOnlyShardIndex) error

	// HasFullShardIndex returns true if we have a full shard Index for the sharded DAG with the given shard key, false otherwise.
	HasFullShardIndex(shard_key string) (bool, error)

	// GetFullShardIndex returns a read only view of the full shard Index for the sharded DAG with the given shard key if we have it.
	GetFullShardIndex(shard_key string) (ReadOnlyShardIndex, error)

	// ShardCidsIterator returns an iterator for all the Cids present in the sharded DAG with the given shard key.
	// Note that the ordering of the Cids in the iterator is undefined.
	ShardCidsIterator(shard_key string) (CidIterator, error)

	// DeleteFullShardIndex removes the full shard Index for the sharded DAG with the given shard key.
	DeleteFullShardIndex(shard_key string) error

	// TODO: Get all cids across all DAGs etc...
}

type CidIterator interface {
	Next() (cid.Cid, error)
	HasNext() (bool, error)
}

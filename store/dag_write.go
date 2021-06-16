package dagstore

import (
	"context"
	"io"
)

// DAGCarFetcher allows a caller to read a CAR for a DAG.
// This abstraction enables the DAG store to fetch and stream a DAG CAR from any source.
// This abstraction simply returns a raw byte stream. It is the caller's responsibility to determine the CAR version and parse the CAR file.
// TODO: Should we use an `io.ReaderAt` here instead of an `io.Reader` to serve random access queries directly out of the
//	 //  source instead of having to duplicate it ?
type DAGCarFetcher interface {
	Fetch(ctx context.Context) (io.ReadCloser, error)

	// called by the DAG Store to determine if this shard is still active
	// for eg: Is the USB still unplugged ? Is the storage deal active ?
	IsActive() bool

	// dispose  transient artefacts produced by retrieval.
	DisposeUponRetrieval() error
}

// DAGWrite is the write side of te sharded DAG store.
type DAGWrite interface {
	// AddShard adds a sharded DAG with the given shard key that can be streamed and fetched using the given fetcher to the DAG store.
	// It will create and persist an Index for the shard if it dosen't already have an Index for the sharded DAG with the given shard key.
	//
	// Implementation Notes:
	//  Do we already have an Index for the shard ? If yes, no Indexing to do, Else
	//  Inspect the given CAR file to know if it already has an Index ? If yes, memoize it or else ask go-car to create one for us
	// 	and memoize it so we don't need to create it again.
	ActivateShard(shard_key string, fetcher DAGCarFetcher) error

	// TODO Need to replace the above `ActivateShard` with this:
	// Atomic get and set.
	// ActivateShardIfNotActive will activate the shard and index it if it's NOT already active.
	// This will be a no-op if the shard is already active.
	// Will return true if the shard was newly activated by this call, false if it was already activated.
	ActivateShardIfNotActive(shard_key string, fetcher DAGCarFetcher) (bool, error)

	// RemoveShard removes the sharded DAG with the given shard key from the DAG store.
	// If dropIndices is set to true, it will also drop all associated indices.
	RemoveShard(shard_key string, dropIndices bool) error
}

package dagstore

import (
	"io"

	"github.com/ipfs/go-cid"
)

type DAGStore interface {
	DAGRead
	DAGWrite
}

// DAGRead defines the interface for reading DAGs from the sharded DAG store.
type DAGRead interface {
	// GetShardReadOnlyBlockstore returns a ReadOnlyBlockStore that allows random read-only lookups
	// in the form of {cid: raw block data} for the sharded DAG with the given shard key.
	GetShardReadOnlyBlockstore(shard_key string) (ReadOnlyBlockStore, error)

	// TODO
	// 	// 1. Ability to lookup any Cid without knowing the sharded DAG it belongs to.
	//  // 2. Get a ReadOnlyBlockStore for DAGs spanning across shard boundaries.
}

// ReadOnlyBlockStore allows clients to stream the raw block data for a given Cid.
// It is the client's responsibility to call Close once the reads are done.
// TODO The current idea is to have a Close function that will clean up
//  //  the CAR file fetched from source/artefacts produced or copied as a part of creating the block store wrapper interface etc.
type ReadOnlyBlockStore interface {
	Get(c cid.Cid) (io.Reader, error)
	Close() error
}

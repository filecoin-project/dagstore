package index

// IndexStore manages and allows access to all Indices built on top of the all the DAGs in the sharded DAG store.
type IndexStore interface {
	FullShardIndexStore

	// TODO: Semantic Indexing -> Does it even belong here ? Dosen't help the DAG Store with serving retrievals.
	// TODO: Cross Shard Indexing
	// ...
}

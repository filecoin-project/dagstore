## CARv2 + DAG store implementation notes

## Overview

The purpose of the CARv2 + DAG store endeavour is to eliminate overhead from the
deal-making processes with the mission of unlocking scalability, performance,
and resource frugality on both miner and client side within the Filecoin
network.

Despite serving Lotus/Filecoin immediate needs, we envision the DAG store to be
a common interplanetary building block across IPFS, Filecoin, and IPLD-based
projects.

The scalability of the former two is bottlenecked by the usage of Badger as a
monolithic blockstore. The DAG store represents an alternative that leverages
sharding and the concept of detachable "data cartridges" to feed and manage data
as transactional units.

For the project that immediately concerns us, the definition of done is the
removal of the Badger blockstore from all client and miner codepaths involved in
storage and retrieval deals, and the transition towards interfacing directly
with low-level data repository files known as CARs (Content ARchives), on both
the read and the write sides.

Note that Badger is generally a fitting database for write-intense and
iterator-heavy workloads. It just doesn't withstand the pattern of usage we
subject it to in large IPLD-based software components, especially past the 100s
GBs data volume mark.

For more information on motivation and architecture, please refer to
[CAR-native DAG store](https://docs.google.com/document/d/118fJdf8HGK8tHYOCQfMVURW9gTF738D0weX-hbG-BvY/edit#). This document is 
recommended as a pre-read.

## Overview

The DAG store comprises three layers:

1. Storage layer (manages shards)
2. Index repository (manages indices)
3. DAG access layer (manages queries)

## Storage layer

The DAG store holds shards of data. In Filecoin, shard = deal. Each shard is a
repository of data capable of acting as a standalone blockstore.

### Shards

Shards are identified by an opaque byte string: the shard key. In the case of
Filecoin, the shard key is the `PieceCID` of the storage deal.

A shard contains:

1. the shard key (identifier).
2. the means to obtain shard data (a `Mount` that provides the CAR either
   locally, remotely, or some other location).
3. the shard index (both acting as a manifest of the contents of the shard, and
   a means to efficiently perform random reads).

### CAR formats

A shard can be filled with CARv1 and CARv2 data. CARv2 can be indexed or
indexless.

The choice of version and characteristics affects how the shard index is
populated:

1. CARv1: the index is calculated upon shard activation.
2. Indexless CARv2: the index is calculated upon shard activation.
3. Indexed CARv2: the inline index is adopted as-is as the shard index.

### Shard states

1. **Available:** the system knows about this shard and is capable of serving
   data from it instantaneously, because (a) an index exists and is loaded, and
   (b) data is locally accessible (e.g. it doesn't need to be fetched from a
   remote mount).
2. **Unavailable:** the system knows about this shard, but is not capable of
   serving data from it because the shard is being initialized, or the mount is
   not available locally, but still accessible with work (e.g. fetching from a
   remote location).
3. **Destroyed**: the shard is no longer available; this is permanent condition.

### Operations

#### Shard registration

To register a new shard, call:

```go
dagst.RegisterShard(key []byte, mount Mount, opts ...RegisterOpts) error
```

1. This method takes a shard key and a `Mount`.
2. It initializes the shard in `Unavailable` state.
3. Calls `mount.Info()` to determine if the mount is of local or remote type.
4. Calls `mount.Stat()` to determine if the mount target exists. If not, it
   errors the registration.
5. If remote, it fetches the remote resource into the scrap area.
6. It determines the kind of CAR it is, and populates the index accordingly.
7. Sets the state to `Available`.
8. Returns.

This method is _synchronous_. It returns only when the shard is fully indexed
and available for serving data. This embodies the consistency property of the
ACID model, whereby an INSERT is immediately queriable upon return.

_RegisterOpts is an extension point._ In the future it can be used to pass in an
unseal/decryption function to be used on access (e.g. such as when fast, random
unseal is available).

#### Shard destruction

To destroy a shard, call:

```go
dagst.DestroyShard(key []byte) (destroyed bool, err error)
```

This erases transient copies from the scrap area, and it removes the shard/mount
from the shard persistence store.

#### Other operations

  * `[Pin/Unpin]Shard()`: retains the shard data in the local scrap area.
  * `ReleaseShard()`: dispose of / release local scrap copies or other resources
    on demand (e.g. unsealed copy).
  * `[Lock/Unlock]Shard()`: for exclusive access.
  * `[Incr/Decr]Shard()`: refcounting on shards.

### Mounts

Shards can be located anywhere, and can come and go dynamically e.g. Filecoin
deals expire, removable media is attached/detached, or the IPFS user purges
content.

It is possible to mount shards with CARs accessible through the local
filesystem, detachable mounts, NFS mounts, distributed filesystems like
Ceph/GlusterFS, HTTP, FTP, etc.

This versatility is provided by an abstraction called `mount.Mount`, a pluggable
component which encapsulates operations/logic to:

1. `Fetch() (io.ReadSeekCloser, error)` Load a CAR from its origin.
2. `Info() mount.Info` Provides info about the mount, e.g. whether it's local or
   remote. This is used to determine whether the fetched CAR needs to be copied
   to a scrap area. It also indicates whether the stream supports seeking.
   Calling `Seek()` on an unseekable stream will panic. `Seek()` is used to
   fast-forward to an index if only that structure needs to be consumed.
3. `Stat() (mount.Stat, error)` Equivalent to a filesystem stat, provides
   information about the target of the mount: whether it exists, size, etc.

When instantiating `Mount` implementations, one can provide credentials, access
tokens, or other parameters through the implementation constructors. This is
necessary if access to the CAR is permissioned/authenticated.

**Local filesystem Mount**

A local filesystem `Mount` implementation loads the CAR directly from the
filesystem file. It is of `local` type and therefore requires no usage of scrap
area.

*This `Mount` is provided out-of-box by the DAG store.*

**Lotus Mount implementation**

A Lotus `Mount` implementation would be instantiated with a sector ID and a
bytes range within the sealed sector file (i.e. the deal segment).

Loading the CAR consists of calling the worker HTTP API to fetch the unsealed
piece. Because the mount is of `remote` type, the DAG store will need to store
it in a local scrap area. Currently, this may lead to actual unsealing on the
Lotus worker cluster through the current PoRep (slow) if the piece is sealed.

With a future PoRep (cheap, snappy) PoRep, sealing can be performed _just_ for
the blocks that are effectively accessed, potentially during IPLD block access
time. A transformation function may be provided in the future as a `RegisterOpt`
that conducts the unsealing.

A prerequisite to enable unsealing-on-demand possible is PoRep and index
symmetry, i.e. the byte positions of blocks in the sealed CAR must be
identical to those in the unsealed CAR.

*This `Mount` is provided by Lotus, as it's implementation specific.*

#### URL representation and registry

Mounts are stateless objects, serialized as URL for persistence and
configuration. This enables us to:

1. Record mounts in shard persistence (see below).
2. Add shards by configuration (e.g. imagine a program whose shards are
   configured in a configuration file)

The URL format mapping is:

```
scheme://host[:port]?key1=value1&key2=value2[&...]
```

- `scheme` is a unique identifier for the mount type, e.g. `lotus://`,
  `file://`, `http://`.
- the rest is scheme-dependent.
  - `host[:port]` is usually the main component of the mount, e.g. a file path,
    a sector ID, etc.
  - query parameters map to mount options (e.g. credentials, more)

Scheme -> implementation bindings are kept in a registry.

### Shard representation and persistence

The shard catalogue needs to survive restarts. Thus, it needs to be persistent.
Options to explore here include LMDB, BoltDB, or others. Here's what the
persisted shard entry could look like:

```go
type PersistedShard struct {
   Key   []byte
   // Mount is a URL representation of the Mount, e.g.
   //   file://path/to/file?opt=1&opt=2
   //   lotus://sectorID?offset=1&length=2
   Mount string
   // LocalPath is the path to the local replica of the CAR in the scrap area, 
   // if the mount if of remote type.
   LocalPath string
}
```

Upon starting, the DAG store will load the catalogue from disk and will
reinstantiate the shard catalogue, the mounts, and the shard states.

### Scrap area

When dealing with remote mounts (e.g. Filecoin storage cluster), the DAG store
will need to copy the remote CAR into local storage to be able to serve DAG
access queries. These copies are called _transient copies_.

Readers access shards from the storage layer by calling
`Acquire/ReleaseShard(key []byte)` methods, which drive the copies into scrap
storage and the deletion of resources.

These methods will need to do refcounting. When no readers are accessing a
shard, the DAG store is free to release local resources. In a first version,
this may happen instantly. In future versions, we may introduce some active
management of the scrap area through usage monitoring + GC. Storage space
assigned to the scrap area may by configuration in the future.

## Index repository

The index repository is the subcomponent that owns and manages the indices in
the DAG store.

There exists three kinds of indices:

1. **Full shard indices.** Consisting of `{ CID: offset }` mappings. In indexed
   CARv2, these are extracted from the inline indices. In unindexed CARv2 and
   CARv1, these are computed using the [Carbs library](https://github.com/willscott/carbs), or the CARv2 upgrade path.
   
   Full shard indices are protected, and not writable externally. Every
   available/unavailable shard MUST have a full shard index. Upon shard
   destruction, its associated full shard index can be disposed.
   
2. **Semantic indices.** Manifests of externally-relevant CIDs contained within
   a shard, i.e. `[]CID` with no offset indication. In other words, subsets of
   the full-shard index with no offset indication.
   
   These are calculated externally (e.g. semantic indexing service) and supplied
   to the DAG store for storage and safekeeping.

   A shard can have infinite number of semantic indices associated with it. Each
   semantic index is identifid and stamped with its generation data (rule and
   version).

   We acknowledge that semantic indices perhaps don't belong in the DAG store
   long-term. Despite that, we decide to incubate them here to potentially spin
   them off in the future.

3. **Top-level cross-shard index.** Aggregates of full shard indices that enable
   shard routing of reads for concrete CIDs.

### Interactions

This component receives the queries coming in from the miner-side indexing
sidecar, which in turn come from network indexers.

It also serves the DAG access layer. When a shard is registered/acquired in the
storage layer, and a Blockstore is demanded for it, the full index to provide
random-access capability is obtained from the index repo.

In the future, this subcomponent will also serve the top-level cross-shard
index.

### Persistence and reconciliation

Indices will be persisted on disk. 





### Interface

```go
type IndexRepo interface {
   FullIndexRepo
   ManifestRepo
}

type FullIndexRepo interface {
   // public
   GetFullIndex(key shard.Key) (FullIndex, error)

   // private, called only by shard management when registering
   // and destroying shards
   InsertFullIndex(key shard.Key, index FullIndex) error
   DeleteFullIndex(key shard.Key) (bool, error)
}

type ManifestRepo interface {
   // public
   GetManifest(key ManifestKey) (Manifest, error)
   InsertManifest(key ManifestKey, manifest Manifest) error
   DeleteManifest(key ManifestKey) (bool, error)
   ExistsManifest(key ManifestKey) (bool, error)
}

type ManifestKey struct {
   Shard    key.Shard
   Rule     string
   Version  string
}

type FullIndex interface {
   Offset(c cid.Cid) (offset int64, err error) // inexistent: offset=-1, err=nil
   Contains(c cid.Cid) (bool, error)
   Len() (l int64, err error)
   ForEach(func(c cid.Cid, offset int64) (ok bool, err error)) error
}
```

## DAG access layer

This layer is responsible for serving DAGs or sub-DAGs from one or many shards.
Initially, queries will require the shard key. That is, queries will be bounded
to a single identifiable shard.

In the future, when the cross-shard top-level index is implemented, the DAG
store will be capable of resolving the shard for any given root CID.

DAG access layer allows various access abstractions:
   1. Obtaining a Blockstore bound to a single shard.
   2. Obtaining a Blockstore bound to a specific set of shards.
   3. Obtaining a global Blockstore.
   4. `ipld.Node` -- TBD.

At this point, we are only concerned with (1). The remaining access patterns
will be elaborated on in the future.

```go
type DAGAccessor interface {
   Shard(key shard.Key) ShardAccessor
}

type ShardAccessor interface {
   Blockstore() (ReadBlockstore, error)
}
```

## Requirements for CARv2

- Index needs to be detachable.
- Index offsets need to be relative to the CARv1, and not absolute in the
  physical CARv2 file.
- Index needs to be iterable.
- Given any CAR file, we should be able to determine its version and
  characteristics (concretely, indexed or not indexed).
- Given a CARv1 file, it should be possible to generate a CARv2 index for it in
  detached form.
- Given a CARv2 file, we should be able to decompose it to the corresponding
  CARv1 payload and the Carbs index. The CAR payload should be exposed with
  `io.ReaderAt` and `io.Reader` abstractions.
- Given an indexed CARv2 file, it should be possible to instantiate a
  `ReadBlockstore` on it in a self-sufficient manner.
- Given a CARv1 or unindexed CARv2, it should be possible to instantiate a
  `ReadBlockstore` with an index provided by us.
- It should be possible to write an indexed CARv2 in streaming fashion. The
  CARv2 library should do the right thing depending on the declared write
  characteristics and the output characteristics. For example:
   1. the writer may declare that blocks are provided in depth-first order and
      without uniqueness guarantees, but may state they want the output to be in
      depth-first order AND deduplicated. In this case, the CARv2 library must
      use the index to prevent duplicate writes (this is the priority case right
      now).
   2. the writer may declare blocks are provided in no order and without
      uniqueness guarantees, but may wish to produce an output in depth-first
      order and deduplicated. In this case, when finalizing the CAR, the library
      must evaluate if a rewrite is necessary, and, if so, it should conduct it
      (this case is not a priority now, but needed down the line for feature
      completeness).

## Discussion: migration from current architecture

TBD.

## Open points

- ...
- ...
- ...

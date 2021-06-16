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
2. the shard data (a CAR file, which can be locally present or absent).
3. the shard index (both acting as a manifest of the contents of the shard, and
   a means to efficiently perform random reads).

### CAR formats

A shard can be filled with CARv1 and CARv2 data. CARv2 can be indexed or
indexless. This affects how the shard index is populated:

1. CARv1: the index is calculated upon shard activation.
2. Indexless CARv2: the index is calculated upon shard activation.
3. Indexed CARv2: the inline index is adopted as-is as the shard index.

### Shard states

1. **Available:** the system knows about this shard and is capable of serving
   data from it instantaneously, because (a) an index exists and is loaded, and
   (b) data is locally accessible (e.g. it doesn't need to be fetched from a
   remote mount).
2. **Unavailable:** the system knows about this shard, but is not capable of
   serving data from it because the shard is being initialized, or the mount is not available locally, but still accessible with work (e.g. fetching from a remote location).
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

_RegisterOpts is an extension point._ In the future it can be used to pass in an
unseal/decryption function to be used on access (e.g. such as when fast, random
unseal is available).

#### Shard destruction

To destroy a shard, call:

```go
dagst.DestroyShard(key []byte) (destroyed bool, err error)
```

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

1. `Fetch() (io.ReadCloser, error)` Load a CAR from its origin.
2. `Info() mount.Info` Provides info about the mount, e.g. whether it's local or
   remote. This is used to determine whether the fetched CAR needs to be copied
   to a scrap area.
3. `Stat() (mount.Stat, error)` Equivalent to a filesystem stat, provides
   information about the target of the mount: whether it exists, size, etc.

When instantiating `Mount` implementations, one can provide credentials, access
tokens, or other parameters through the implementation constructors. This is
necessary if access to the CAR is permissioned/authenticated.

**Local filesystem Mount**

A local filesystem `Mount` implementation loads the CAR directly from the
filesystem file. It is of `local` type and therefore requires no usage of scrap area.

*This `Mount` is provided out-of-box by the DAG store.*

**Lotus Mount implementation**

A Lotus `Mount` implementation would be instantiated with a sector ID and a
bytes range within the sealed sector file (i.e. the deal segment).

Loading the CAR consists of calling the worker HTTP API to fetch the unsealed
piece. Because the mount is of `remote` type, the DAG store will need to store it in a local scrap area. Currently, this may lead to actual unsealing on
the Lotus worker cluster through the current PoRep (slow) if the piece is
sealed.

With a future PoRep (cheap, snappy) PoRep, sealing can be performed _just_ for
the blocks that are effectively accessed, potentially during IPLD block access
time. A transformation function may be provided in the future as a
`RegisterOpt`.

*This `Mount` is provided by Lotus, as it's implementation specific.*

### Shard representation and persistence

The shard catalogue needs to survive restarts. Thus, it needs to be persistent.
Options to explore here include LMDB, BoltDB, or others. Here's what the persisted shard entry could look like:

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

Upon starting, the DAG store will load the catalogue from disk and will reinstantiate the shard catalogue, the mounts, and the shard states.

### Scrap area

When dealing with remote mounts (e.g. Filecoin storage cluster), the DAG store
will need to copy the remote CAR into local storage to be able to serve DAG
access queries.

Readers access shards from the storage layer by calling
`Acquire/ReleaseShard(key []byte)` methods, which drive the copies into scrap
storage and the deletion of resources.

These methods will need to do refcounting. When no readers are accessing a
shard, the DAG store is free to release local resources. In a first version,
this may happen instantly. In future versions, we may introduce some active
management of the scrap area through usage monitoring + GC. Storage space assigned to the scrap area may by configuration in the future.

---

# RAW CONTENT 🐉

## Index repository

1. Stores different types of indices, always associated with a shard.
   - full shard indices: protected, not writable externally, managed entirely by
     the storage layer { CID: offset } mappings.
   - semantic indices: supplied externally (by markets/indexing process).
     They are really CID manifests.
   - other indices in the future?
2. Semantic indices are really not indices, they are manifests. Maybe they don't
   belong here, but they need to be somewhere, and this is a good place to
   incubate them and potentially spin them off in the future.
3. Ultimately serves network indexer requests. These requests come in through
   the markets/indexing process, and arrive here.
4. In the future, will manage the cross-shard top-level index, and thus will
   serve as a shard routing mechanism.

## DAG access layer

1. Responsible for serving DAGs or sub-DAGs from one or many shards.
   Initially, queries will require the shard key.
2. In the future, when the cross-shard top-level index is implemented, the
   DAG store will be capable for resolving the shard key for a given root CID.
3. DAG access layer allows various access strategies:
   1. Obtaining a Blockstore bound to a single shard.
   2. Obtaining a Blockstore bound to a specific set of shards.
   3. Obtaining a global Blockstore.
   4. ...

## Requirements for CARv2

- Index needs to be detachable.
- Index offsets need to be relative to the CARv1, and not absolute in the
  physical CARv2 file.
- Index needs to be iterable.

## TODO

Refcounts CARv1
PoRep unseal on demand.
DAG store does not physically own CAR files, it tracks them and they can disappear from under it at any point in time.
Danging references in shard => across shards.
shard registration is synchronous
DAG store knows how to process CARv1, CARv2.
AddShard is synchronous: error, useless if not. DB guarantees reading after writing.
locking and transactionality
transient copies
migration


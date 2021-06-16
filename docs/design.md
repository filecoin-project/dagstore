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
2. the shard data (a CAR file).
3. the shard index (both acting as a manifest of the contents of the shard, and
   a means to efficiently perform random reads).

A shard can be filled with CARv1 and CARv2 data. CARv2 can be indexed or
indexless. This affects how the shard index is populated:

1. CARv1: the index is calculated upon shard activation.
2. Indexless CARv2: the index is calculated upon shard activation.
3. Indexed CARv2: the inline index is adopted as-is as the shard index.

### CAR mounting

A key property for scaling the DAG store is CAR location independence and
ephemerality.

CARs can be located anywhere, and can come and go dynamically (e.g. as removable
media is attached/detached, Filecoin deals expire, or the IPFS user purges
content).

It is possible to mount shards with CARs accessible through the local
filesystem, detachable mounts, NFS mounts, distributed filesystems like
Ceph/GlusterFS, HTTP, FTP, etc.

This versatility is provided by an abstraction called `Tether`, a pluggable
component which encapsulates operations/logic to:

1. `Load() (io.ReadCloser, error)` Load a CAR, optionally fetching it from a
   remote location into a scrap area.
2. `Accessible() (bool, error)` Test whether the CAR is accessible locally and
   at origin (used to determine if the shard is mounted or unmounted).
3. Transform the origin CAR on access (e.g. by performing an unseal operation).
4. `Dispose() (bool, error)` Dispose of / release local scrap copies or other
   resources on demand (e.g. unsealed copy). This is invoked when unmounting or
   destroying the shard.

When instantiating Tether implementations, one can provide credentials, access
tokens, or other parameters through constructors. This is necessary if access to
the CAR is permissioned/authenticated.

**Local filesystem Tether implementation**

A local filesystem `Tether` implementation loads the CAR directly from the
filesystem file, and would utilise no scrap area. The test would consist of a
simple `os.Stat()` operation. The disposal would noop, as no temporary resources
are used.

*This `Tether` is provided out-of-box by the DAG store.*

**Lotus Tether implementation**

A Lotus `Tether` implementation would be instantiated with a sector ID and a
bytes range within the sealed sector file (i.e. the deal segment).

Loading the CAR consists of calling the worker HTTP API to fetch the unsealed
piece into a local scrap area. Currently, this may lead to actual unsealing on
the Lotus worker cluster through the current PoRep (slow) if the piece is
sealed.

With a future PoRep (cheap, snappy) PoRep, sealing can be performed _just_ for
the blocks that are effectively accessed, during access time. Thus, the
unsealing operation would be called **in the DAG store** as an on-access
transformation, and not on the Lotus worker cluster.

*This `Tether` is provided by Lotus, as it's implementation specific.*

# RAW CONTENT (WIP)

### Shard states

3. Shards are like data cartridges that come and go. A shard can be:
   1. Active: the system knows about this shard and is fully capable of
      serving data from it instantaneously, because the CAR is immediately
      accessible (e.g. it doesn't need to be fetched from a remote location,
      nor has it been inactivated), and an index exists (either embedded in the CAR
      or as an external artefact).
   2. Inactive: the system knows about this shard, but is not capable of serving
      data from it because the shard is still being initialized (fetched from
      its location, or its index calculated if it's a CARv1 or an indexed CARv2),
      or it is gone (e.g.the CAR has been deleted). However, if the shard is
      reactivated (the CAR is brought back in, such as via unsealing), data can
      be served immediately.
   3. Destroyed: the shard is no longer available permanently.

Synchronous activation

1. fetch the CAR from its location
2. subject it to a transformation (e.g. unsealing)
3. dispose of the CAR

In Lotus, the `Fetcher` is responsible for fetching the deal CAR from workers and 

Local paths are currently prioritised over other forms. It is presumed
     that networked FS will be mounted to local paths.
   - The fetching of the CAR resource is done via a Getter abstraction that
     returns an io.ReadCloser.
1. A shard initially created with an indexed CARv2 (e.g. a new deal transfer)
   can be deactivated (e.g. such if the unsealed copy is deleted) and later
   reactivated with a CARv1 equivalent (unsealed from sealed sector), without
   index recreation.
   - This is possible because the original index is stored in the index repo and
     can be joined with the CARv1 copy.


### Interaction with sealed sectors

1. The DAG store does not manage unsealing. When a piece has to be unsealed,
   to serve a deal, the unsealer must reactivate the shard with the unsealed
   CARv1, which can be supplied as a slice (offset + length) of an unsealed
   sector. The DAG store will recover the index from the index repo.

## Index repository

1. Stores different types of indices, always associated with a shard.
   - full shard indices: protected, not writable externally, managed entirely by
     the storage layer. { CID: offset } mappings.
   - semantic indices: supplied externally (by markets/indexing process).
     CID manifests.
   - other indices in the future?
2. Semantic indices don't belong here.
3. Ultimately serves network indexer requests. These requests come in through the
   markets/indexing process, and arrive here.
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
   4. 

## Requirements for CARv2

- Index needs to be detachable.
- Index offsets need to be relative to the CARv1, and not absolute in the
  physical CARv2 file.
- Index needs to be iterable.

## Brainstorm

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


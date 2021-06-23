package mount

import (
	"context"
	"io"
	"net/url"
)

// MountKind is an enum describing the kind of Mount.
type MountKind int

const (
	// MountKindLocal indicates that the asset represented by this mount is of
	// local provenance (e.g. filesystem mount). A call to Fetch() will open a
	// local stream.
	//
	// Note that mounts of this kind may be indirectly backed by remote storage
	// (e.g. NFS, FUSE), but from the viewpoint of the DAG store, the resource
	// is considered local.
	MountKindLocal MountKind = iota

	// MountKindRemote indicates that the asset represented by this mount is
	// fetched from a remote provenance (e.g. HTTP, Filecoin sealing cluster,
	// IPFS, etc.) A call to Fetch() is likely to download the asset from
	// a remote source, thus it is advisable to cache the asset locally once
	// downloaded.
	MountKindRemote
)

// Mount is a pluggable component that represents the original location of the
// data contained in a shard known to the DAG store. The backing resource is a
// CAR file.
//
// Shards can be located anywhere, and can come and go dynamically e.g. Filecoin
// deals expire, removable media is attached/detached, or the IPFS user purges
// content.
//
// It is possible to mount shards with CARs accessible through the local
// filesystem, detachable mounts, NFS mounts, distributed filesystems like
// Ceph/GlusterFS, HTTP, FTP, etc.
//
// Mount implementations are free to define constructor parameters or setters
// to supply arguments needed to initialize the mount, such as credentials,
// sector IDs, CIDs, etc.
//
// Mounts must define a deterministic URL representation which will be used to:
//
//  a. deserialise the Mount from DAG persistence when resuming the system by using a pre-defined Mount factory mapped to the URL scheme.
//  b. support adding mounts from configuration files.
type Mount interface {
	// Fetch fetches the asset from its original location, returning a read only
	// stream. This may be a remote or local stream.
	Fetch(ctx context.Context) (io.ReadCloser, error)

	// Info describes the Mount. This is a pure function.
	Info() Info

	// Stat describes the remote resource.
	Stat() (Stat, error)
}

// Info describes a mount.
type Info struct {
	Kind MountKind
	URL  *url.URL
}

type Stat struct {
	Exists bool
	Size   uint64
}

// MountFactory allows instantiation of a Mount from the deterministic URL representation defined by the Mount.
type MountFactory interface {
	// Parse initializes the mount from a URL.
	Parse(u *url.URL) (Mount, error)
}

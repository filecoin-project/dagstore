package mount

import (
	"context"
	"errors"
	"io"
	"net/url"
)

var (
	// ErrNotSeekable is returned when FetchSeek is called on a mount that is
	// not seekable.
	ErrNotSeekable = errors.New("mount not seekable")
)

// Source is an enum describing the source of a Mount.
type Source int

const (
	// SourceLocal indicates that the asset represented by this mount is of
	// local provenance (e.g. filesystem mount). A call to Fetch() will open a
	// local stream.
	//
	// Note that mounts of this kind may be indirectly backed by remote storage
	// (e.g. NFS, FUSE), but from the viewpoint of the DAG store, the resource
	// is considered local.
	SourceLocal Source = iota

	// SourceRemote indicates that the asset represented by this mount is
	// fetched from a remote provenance (e.g. HTTP, Filecoin sealing cluster,
	// IPFS, etc.) A call to Fetch() is likely to download the asset from
	// a remote source, thus it is advisable to cache the asset locally once
	// downloaded.
	SourceRemote
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
//  a. deserialise the Mount from DAG persistence when resuming the system by
//     using a pre-defined Mount factory mapped to the URL scheme.
//  b. support adding mounts from configuration files.
type Mount interface {
	// Fetch fetches the asset from its original location, returning a read only
	// stream. This may be a remote or local stream.
	Fetch(ctx context.Context) (io.ReadCloser, error)

	// FetchSeek returns a io.ReadSeekCloser if supported. Callers can check for
	// support by looking at Info.Seekable.
	//
	// Calling FetchSeek when Info.Seekable is false is guaranteed to fail with
	// ErrNotSeekable.
	FetchSeek(ctx context.Context) (io.ReadSeekCloser, error)

	// Info describes the Mount. This is a pure function.
	Info() Info

	// Stat describes the remote resource.
	Stat() (Stat, error)
}

// Info describes a mount.
type Info struct {
	// Source indicates the type of source.
	Source Source
	// URL is the canonical URL this Mount serializes to.
	URL *url.URL
	// Seekable indicates whether the mount supports seekable streams, and
	// therefore calling FetchSeek is legal.
	Seekable bool
}

// Stat
type Stat struct {
	Exists bool
	Size   uint64
}

// Type represents a mount type, and allows instantiation of a Mount from its
// URL serialized form.
type Type interface {
	// Parse initializes the mount from a URL.
	Parse(u *url.URL) (Mount, error)
}

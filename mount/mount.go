package mount

import (
	"context"
	"errors"
	"io"
	"net/url"
)

var (
	// ErrSeekUnsupported is returned when Seek is called on a mount that is
	// not seekable.
	ErrSeekUnsupported = errors.New("mount does not support seek")

	// ErrRandomAccessUnsupported is returned when ReadAt is called on a mount
	// that does not support random access.
	ErrRandomAccessUnsupported = errors.New("mount does not support random access")
)

// Kind is an enum describing the source of a Mount.
type Kind int

const (
	// KindLocal indicates that the asset represented by this mount is of
	// transient provenance (e.g. filesystem mount). A call to Fetch() will open a
	// transient stream.
	//
	// Note that mounts of this kind may be indirectly backed by underlying storage
	// (e.g. NFS, FUSE), but from the viewpoint of the DAG store, the resource
	// is considered transient.
	KindLocal Kind = iota

	// KindRemote indicates that the asset represented by this mount is
	// fetched from a underlying provenance (e.g. HTTP, Filecoin sealing cluster,
	// IPFS, etc.) A call to Fetch() is likely to download the asset from
	// a underlying source, thus it is advisable to cache the asset locally once
	// downloaded.
	KindRemote
)

// Mount is a pluggable component that represents the original location of the
// data contained in a shard known to the DAG store. The backing resource is a
// CAR file.
//
// Shards can be located anywhere, and can come and go dynamically e.g. Filecoin
// deals expire, removable media is attached/detached, or the IPFS user purges
// content.
//
// It is possible to mount shards with CARs accessible through the transient
// filesystem, detachable mounts, NFS mounts, distributed filesystems like
// Ceph/GlusterFS, HTTP, FTP, etc.
//
// Mount implementations are free to define constructor parameters or setters
// to supply arguments needed to initialize the mount, such as credentials,
// sector IDs, CIDs, etc.
//
// MountTypes must define a deterministic URL representation which will be used to:
//
//  a. deserialise the Mount from DAG persistence when resuming the system by
//     using a pre-defined Mount factory mapped to the URL scheme.
//  b. support adding mounts from configuration files.
type Mount interface {
	io.Closer

	// Fetch returns a Reader for this mount. Not all read access methods
	// may be supported. Check the Info object to determine which access methods
	// are effectively supported.
	//
	// To seamlessly upgrade a Mount to a fully-featured mount by using a transient
	// transient file, use the Upgrader.
	Fetch(ctx context.Context) (Reader, error)

	// Info describes the Mount. This is a pure function.
	Info() Info

	// Stat describes the underlying resource.
	Stat(ctx context.Context) (Stat, error)

	// Serialize returns a canonical URL that can be used to revive the Mount
	// after a restart.
	Serialize() *url.URL

	// Deserialize configures this Mount from the specified URL.
	Deserialize(*url.URL) error
}

// Reader is a fully-featured Reader returned from MountTypes. It is the
// union of the standard IO sequential access method (Read), with seeking
// ability (Seek), as well random access (ReadAt).
type Reader interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
}

// Info describes a mount.
type Info struct {
	// Kind indicates the kind of mount.
	Kind Kind

	// TODO convert to bitfield
	AccessSequential bool
	AccessSeek       bool
	AccessRandom     bool
}

// Stat
type Stat struct {
	// Exists indicates if the asset exists.
	Exists bool
	// Size is the size of the asset referred to by this Mount.
	Size int64
	// Ready indicates whether the mount can serve the resource immediately, or
	// if it needs to do work prior to serving it.
	Ready bool
}

type NopCloser struct {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (*NopCloser) Close() error {
	return nil
}

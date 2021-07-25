package index

import (
	"errors"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipld/go-car/v2/index"
)

var ErrNotFound = errors.New("index not found")

// Repo is the central index repository object that manages full indices and
// manifests.
type Repo interface {
	FullIndexRepo
	ManifestRepo
}

type Stat struct {
	Exists bool
	Size   uint64
}

type FullIndexRepo interface {
	// GetFullIndex returns the full index for the specified shard.
	GetFullIndex(key shard.Key) (index.Index, error)

	// AddFullIndex persists a full index for a shard.
	AddFullIndex(key shard.Key, index index.Index) error

	// DropFullIndex drops the full index for the specified shard. If the error
	// is nil, it returns whether an index was effectively dropped.
	DropFullIndex(key shard.Key) (dropped bool, err error)

	// StatFullIndex stats a full index.
	StatFullIndex(key shard.Key) (Stat, error)

	// Len returns the number of indices in the repo.
	Len() (int, error)

	// ForEach calls the callback with the key for each index.
	//
	// Returning true from the callback will continue the traversal.
	// Returning false will terminate the traversal.
	//
	// A non-nil error will abort the traversal, and the error will be
	// propagated to the caller.
	ForEach(func(shard.Key) (bool, error)) error

	// Size returns the size of the repo in bytes.
	Size() (uint64, error)
}

// TODO unimplemented.
type ManifestRepo interface {
	// ListManifests returns the available manifests for a given shard,
	// identified by their ManifestKey.
	ListManifests(key shard.Key) ([]ManifestKey, error)

	// GetManifest returns the Manifest identified by a given ManifestKey.
	GetManifest(key ManifestKey) (Manifest, error)

	// AddManifest adds a Manifest to the ManifestRepo.
	AddManifest(key ManifestKey, manifest Manifest) error

	// DropManifest drops a Manifest from the ManifestRepo.
	DropManifest(key ManifestKey) (bool, error)

	// StatManifest stats a Manifest.
	StatManifest(key ManifestKey) (Stat, error)
}

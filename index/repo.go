package index

import (
	"github.com/filecoin-project/dagstore/shard"
)

// Repo is the central index repository object that manages full indices and
// manifests.
type Repo struct {
	FullIndexRepo
	ManifestRepo
}

type Stat struct {
	Exists bool
	Size   uint64
}

type FullIndexRepo interface {
	// GetFullIndex returns the full index for the specified shard.
	GetFullIndex(key shard.Key) (FullIndex, error)

	// AddFullIndex persists a full index for a shard.
	AddFullIndex(key shard.Key, index FullIndex) error

	// DropFullIndex drops the full index for the specified shard. If the error
	// is nil, it returns whether an index was effectively dropped.
	DropFullIndex(key shard.Key) (dropped bool, err error)

	// StatFullIndex stats a full index.
	StatFullIndex(key shard.Key) (Stat, error)
}

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

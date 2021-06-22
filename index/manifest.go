package index

import (
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
)

// ManifestKey identifies a manifest. It is a triple that can act as a composite
// key, comprising the shard key and generation metadata.
type ManifestKey struct {
	Shard      shard.Key
	GenRule    string
	GenVersion uint
}

// Manifest are sets of CIDs with no offset indication.
type Manifest interface {
	// Contains checks whether a given CID is contained in the manifest.
	Contains(c cid.Cid) (bool, error)

	// Len returns the count of entries this manifest has.
	Len() (l int64, err error)

	// ForEach traverses the manifest using an visitor pattern. The supplied
	// callback will be called for each manifest entry, in no particular order.
	//
	// Returning true from the callback will continue the traversal.
	// Returning false will terminate the traversal.
	//
	// A non-nil error will abort the traversal, and the error will be
	// propagated to the caller.
	ForEach(func(c cid.Cid) (ok bool, err error)) error
}

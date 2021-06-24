package index

import (
	"errors"
	"io"

	"github.com/ipfs/go-cid"
)

// ErrNotFound is returned when an entry is not found in an index.
var ErrNotFound = errors.New("entry not found in index")

// TODO we may be able to replace this interface with once it's public, as long
//  as it doesn't leak implementation details.
//  https://github.com/ipld/go-car/blob/2a49cbd958b0c6443dd1e9b518814199bf50842d/v2/internal/index/index.go#L37
type FullIndex interface {
	// Offset returns the offset of a CID inside the data CAR of a shard.
	//
	// If the CID exists, the offset will be zero or positive.
	// If the CID is not contained in the index, it will return an
	// ErrCidNotFound error
	// If an error ocurred, it will be returned.
	Offset(c cid.Cid) (offset uint64, err error)

	// Contains checks whether a given CID is contained in the index.
	Contains(c cid.Cid) (bool, error)

	// Len returns the count of entries this index has.
	Len() (l int64, err error)

	// ForEach traverses the index using an visitor pattern. The supplied
	// callback will be called for each index entry, in no particular order.
	//
	// Returning true from the callback will continue the traversal.
	// Returning false will terminate the traversal.
	//
	// A non-nil error will abort the traversal, and the error will be
	// propagated to the caller.
	ForEach(func(c cid.Cid, offset uint64) (ok bool, err error)) error

	// Marshal writes the index to the given writer.
	Marshal(w io.Writer) error

	// Unmarshal loads the index from the given reader.
	Unmarshal(w io.Reader) error
}

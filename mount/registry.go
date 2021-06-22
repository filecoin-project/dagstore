package mount

import (
	"errors"
	"net/url"
	"reflect"
)

// ErrUnrecognizedScheme is returned by Instantiate() when attempting to
// initialize a Mount with an unrecognized URL scheme.
var ErrUnrecognizedScheme = errors.New("unrecognized mount scheme")

// Registry is a registry of Mount types known to the DAG store.
type Registry struct {
	m map[string]reflect.Type
}

// Register adds a new Mount type to the registry. The mount argument is a dummy
// instance of the Mount type of which we'll retain the reflect.Type token to
// create new instances when requested.
func (r *Registry) Register(scheme string, mount Mount) {
	panic("not implemented")
}

// Instantiate instantites a new Mount from a URL.
//
// It looks up the mount type in the registry based on the URL scheme,
// constructs a new instance, and calls Mount.Parse() to populate it.
//
// If it errors, it propagates the error returned by the Mount. If the scheme
// is not recognized, it returns ErrUnrecognizedScheme.
func (r *Registry) Instantiate(u *url.URL) (Mount, error) {
	panic("not implemented")
}

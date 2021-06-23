package mount

import (
	"errors"
	"net/url"
	"reflect"
	"sync"

	"golang.org/x/xerrors"
)

// ErrUnrecognizedScheme is returned by Instantiate() when attempting to
// initialize a Mount with an unrecognized URL scheme.
var ErrUnrecognizedScheme = errors.New("unrecognized mount scheme")

// ErrRegistrationWithNonPointerType is returned by Register() when attempting to
// register a Mount with a dynamic non-pointer type.
var ErrRegistrationWithNonPointerType = errors.New("Register called with non-pointer type")

// Registry is a registry of Mount types known to the DAG store.
type Registry struct {
	lk sync.RWMutex
	m  map[string]reflect.Type
}

// Register adds a new Mount type to the registry. The mount argument is a dummy
// instance of the Mount type of which we'll retain the reflect.Type token to
// create new instances when requested.
func (r *Registry) Register(scheme string, mount Mount) error {
	r.lk.Lock()
	defer r.lk.Unlock()

	if _, ok := r.m[scheme]; ok {
		return xerrors.New("mount already registered")
	}

	mtype := reflect.TypeOf(mount)
	if mtype.Kind() != reflect.Ptr {
		return ErrRegistrationWithNonPointerType
	}

	r.m[scheme] = mtype.Elem()
	return nil
}

// Instantiate instantiates a new Mount from a URL.
//
// It looks up the mount type in the registry based on the URL scheme,
// constructs a new instance, and calls Mount.Parse() to populate it.
//
// If it errors, it propagates the error returned by the Mount. If the scheme
// is not recognized, it returns ErrUnrecognizedScheme.
func (r *Registry) Instantiate(u *url.URL) (Mount, error) {
	r.lk.RLock()
	defer r.lk.RUnlock()

	typ, ok := r.m[u.Scheme]
	if !ok {
		return nil, ErrUnrecognizedScheme
	}

	mt, ok := reflect.New(typ).Interface().(Mount)
	if !ok {
		return nil, xerrors.New("registry could not construct value of type Mount")
	}

	// instantiate the mount state from the URL.
	if err := mt.Parse(u); err != nil {
		return nil, xerrors.Errorf("failed to instantiate mount, mt.Parse, err=%s", err)
	}

	return mt, nil
}

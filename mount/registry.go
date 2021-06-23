package mount

import (
	"errors"
	"net/url"
	"sync"

	"golang.org/x/xerrors"
)

// ErrUnrecognizedScheme is returned by Instantiate() when attempting to
// initialize a Mount with an unrecognized URL scheme.
var ErrUnrecognizedScheme = errors.New("unrecognized mount scheme")

// Registry is a registry of Mount factories known to the DAG store.
type Registry struct {
	lk sync.RWMutex
	m  map[string]MountFactory
}

// Register adds a new Mount factory to the registry and maps it against the given URL scheme.
func (r *Registry) Register(scheme string, mount MountFactory) error {
	r.lk.Lock()
	defer r.lk.Unlock()

	if _, ok := r.m[scheme]; ok {
		return xerrors.New("mount factory already registered for scheme")
	}

	r.m[scheme] = mount
	return nil
}

// Instantiate instantiates a new Mount from a URL.
//
// It looks up the Mount factory in the registry based on the URL scheme,
// calls Parse() on it to get a Mount and returns the Mount.
//
// If it errors, it propagates the error returned by the Mount factory. If the scheme
// is not recognized, it returns ErrUnrecognizedScheme.
func (r *Registry) Instantiate(u *url.URL) (Mount, error) {
	r.lk.RLock()
	defer r.lk.RUnlock()

	mft, ok := r.m[u.Scheme]
	if !ok {
		return nil, ErrUnrecognizedScheme
	}

	mt, err := mft.Parse(u)
	if err != nil {
		return nil, xerrors.Errorf("failed to instantiate mount with factory.Parse: %w", err)

	}

	return mt, nil
}

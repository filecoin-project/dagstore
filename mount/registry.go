package mount

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sync"
)

var (
	// ErrUnrecognizedScheme is returned by Instantiate() when attempting to
	// initialize a Mount with an unrecognized URL scheme.
	ErrUnrecognizedScheme = errors.New("unrecognized mount scheme")

	// ErrUnrecognizedType is returned by Encode() when attempting to
	// represent a Mount whose type has not been registered.
	ErrUnrecognizedType = errors.New("unrecognized mount type")
)

// Registry is a registry of Mount factories known to the DAG store.
type Registry struct {
	lk       sync.RWMutex
	byScheme map[string]Mount
	byType   map[reflect.Type]string
}

// NewRegistry constructs a blank registry.
func NewRegistry() *Registry {
	return &Registry{byScheme: map[string]Mount{}, byType: map[reflect.Type]string{}}
}

// Register adds a new mount type to the registry under the specified scheme.
//
// The supplied Mount is used as a template to create new instances.
//
// This means that the provided Mount can contain environmental configuration
// that will be automatically carried over to all instances.
func (r *Registry) Register(scheme string, template Mount) error {
	r.lk.Lock()
	defer r.lk.Unlock()

	if _, ok := r.byScheme[scheme]; ok {
		return fmt.Errorf("mount already registered for scheme: %s", scheme)
	}

	if _, ok := r.byType[reflect.TypeOf(template)]; ok {
		return fmt.Errorf("mount already registered for type: %T", template)
	}

	r.byScheme[scheme] = template
	r.byType[reflect.TypeOf(template)] = scheme
	return nil
}

// Instantiate instantiates a new Mount from a URL.
//
// It looks up the Mount template in the registry based on the URL scheme,
// creates a copy, and calls Deserialize() on it with the supplied URL before
// returning.
//
// It propagates any error returned by the Mount#Deserialize method.
// If the scheme is not recognized, it returns ErrUnrecognizedScheme.
func (r *Registry) Instantiate(u *url.URL) (Mount, error) {
	r.lk.RLock()
	defer r.lk.RUnlock()

	template, ok := r.byScheme[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnrecognizedScheme, u.Scheme)
	}

	instance := clone(template)
	if err := instance.Deserialize(u); err != nil {
		return nil, fmt.Errorf("failed to instantiate mount with url %s into type %T: %w", u.String(), template, err)
	}
	return instance, nil
}

// Represent returns the URL representation of a Mount, using the scheme that
// was registered for that type of mount.
func (r *Registry) Represent(mount Mount) (*url.URL, error) {
	r.lk.RLock()
	defer r.lk.RUnlock()

	// special-case the upgrader, as it's transparent.
	if up, ok := mount.(*Upgrader); ok {
		mount = up.underlying
	}

	scheme, ok := r.byType[reflect.TypeOf(mount)]
	if !ok {
		return nil, fmt.Errorf("failed to represent mount with type %T: %w", mount, ErrUnrecognizedType)
	}

	u := mount.Serialize()
	u.Scheme = scheme
	return u, nil
}

// clone clones m1 into m2, casting it back to a Mount. It is only able to deal
// with pointer types that implement Mount.
func clone(m1 Mount) (m2 Mount) {
	m2obj := reflect.New(reflect.TypeOf(m1).Elem())
	m1val := reflect.ValueOf(m1).Elem()
	m2val := m2obj.Elem()
	for i := 0; i < m1val.NumField(); i++ {
		field := m2val.Field(i)
		if field.CanSet() {
			field.Set(m1val.Field(i))
		}
	}
	return m2obj.Interface().(Mount)
}

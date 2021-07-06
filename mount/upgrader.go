package mount

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// Upgrader is a bridge to upgrade any Mount into one with full-featured
// Reader capabilities, whether the original mount is of remote or local kind.
// It does this by managing a local transient copy.
//
// If the underlying mount is fully-featured, the Upgrader has no effect, and
// simply passes through to the underlying mount.
type Upgrader struct {
	underlying  Mount
	passthrough bool

	lk        sync.Mutex
	transient string
	rootdir   string
}

var _ Mount = (*Upgrader)(nil)

// Upgrade constructs a new Upgrader for the underlying Mount. If provided, it
// will reuse the file in path `initial` as the initial transient copy. Whenever
// a new transient copy has to be created, it will be created under `rootdir`.
func Upgrade(underlying Mount, rootdir, initial string) (*Upgrader, error) {
	ret := &Upgrader{underlying: underlying, rootdir: rootdir}
	if ret.rootdir == "" {
		ret.rootdir = os.TempDir() // use the OS' default temp dir.
	}

	switch info := underlying.Info(); {
	case !info.AccessSequential:
		return nil, fmt.Errorf("underlying mount must support sequential access")
	case info.AccessSeek && info.AccessRandom:
		ret.passthrough = true
		return ret, nil
	}

	if initial != "" {
		if _, err := os.Stat(initial); err == nil {
			ret.transient = initial
			return ret, nil
		}
	}

	return ret, nil
}

func (u *Upgrader) Fetch(ctx context.Context) (Reader, error) {
	if u.passthrough {
		return u.underlying.Fetch(ctx)
	}

	// determine if the transient is still alive.
	u.lk.Lock()
	defer u.lk.Unlock()

	if u.transient != "" {
		if _, err := os.Stat(u.transient); err == nil {
			return os.Open(u.transient)
		}
	}

	// transient appears to be dead, refetch.
	if err := u.refetch(ctx); err != nil {
		return nil, err
	}

	return os.Open(u.transient)
}

func (u *Upgrader) Info() Info {
	return Info{
		Kind:             KindLocal,
		URL:              u.underlying.Info().URL,
		AccessSequential: true,
		AccessSeek:       true,
		AccessRandom:     true,
	}
}

func (u *Upgrader) Stat(ctx context.Context) (Stat, error) {
	if u.transient != "" {
		if stat, err := os.Stat(u.transient); err == nil {
			ret := Stat{Exists: true, Size: stat.Size()}
			return ret, nil
		}
	}
	return u.underlying.Stat(ctx)
}

// TransientPath returns the local path of the transient file. If the Upgrader
// is passthrough, the return value will be "".
func (u *Upgrader) TransientPath() string {
	u.lk.Lock()
	defer u.lk.Unlock()

	return u.transient
}

func (u *Upgrader) Close() error {
	panic("implement me")
}

func (u *Upgrader) refetch(ctx context.Context) error {
	if u.transient != "" {
		_ = os.Remove(u.transient)
	}
	file, err := os.CreateTemp(u.rootdir, "transient")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer file.Close()

	u.transient = file.Name()

	// sanity check on underlying mount.
	if stat, err := u.underlying.Stat(ctx); err != nil {
		return fmt.Errorf("underlying mount stat returned error: %w", err)
	} else if !stat.Exists {
		return fmt.Errorf("underlying mount no longer exists")
	}

	// fetch from underlying and copy.
	from, err := u.underlying.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch from underlying mount: %w", err)
	}
	defer from.Close()

	_, err = io.Copy(file, from)
	if err != nil {
		return fmt.Errorf("failed to copy underlying mount to transient file: %w", err)
	}

	return nil
}

// DeleteTransient deletes the transient associated with this Upgrader, if
// one exists. It is the caller's responsibility to ensure the transient is
// not in use.
func (u *Upgrader) DeleteTransient() error {
	u.lk.Lock()
	defer u.lk.Unlock()

	if u.transient == "" {
		return nil // nothing to do.
	}

	err := os.Remove(u.transient)
	if err != nil {
		return err
	}

	u.transient = ""
	return nil
}

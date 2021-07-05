package mount

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// Upgrader serves as a bridge to upgrade a Mount into one with full-featured
// Reader capabilities. It does this by caching a transient copy as file if
// the original mount type does not support all access patterns.
//
// If the underlying mount is already fully-featured, the Upgrader is
// acts as a noop.
//
// TODO perform refcounts so we can track inactive transient files.
// TODO provide root directory for temp files (or better: temp file factory function).
type Upgrader struct {
	underlying  Mount
	passthrough bool

	lk        sync.Mutex
	transient string
	// TODO refs int
}

var _ Mount = (*Upgrader)(nil)

// Upgrade constructs a new Upgrader for the underlying Mount.
func Upgrade(underlying Mount, initial string) (*Upgrader, error) {
	ret := &Upgrader{underlying: underlying}

	info := underlying.Info()
	if !info.AccessSequential {
		return nil, fmt.Errorf("underlying mount must support sequential access")
	}
	if info.AccessSeek && info.AccessRandom {
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

func (u *Upgrader) Close() error {
	panic("implement me")
}

func (u *Upgrader) refetch(ctx context.Context) error {
	if u.transient != "" {
		_ = os.Remove(u.transient)
	}
	file, err := os.CreateTemp("dagstore", "transient")
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

	_, err = io.Copy(file, from)
	if err != nil {
		return fmt.Errorf("failed to copy underlying mount to transient file: %w", err)
	}

	return nil
}

//
// // Clean removes any transient assets.
// func (m *Upgrader) Clean() error {
// 	s.Lock()
// 	defer s.Unlock()
//
// 	// check if we have readers and refuse to clean if so.
// 	if s.refs != 0 {
// 		return fmt.Errorf("failed to delete shard: %w", ErrShardInUse)
// 	}
//
// 	if s.transient == nil {
// 		// nothing to do.
// 		return nil
// 	}
//
// 	// we can safely remove the transient.
// 	_ = s.transient.Close()
// 	err := os.Remove(s.transient.Name())
// 	if err == nil {
// 		s.transient = nil
// 	}
//
// 	// refresh the availability.
// 	_, _ = s.refreshAvailability(nil)
// 	return nil
// }

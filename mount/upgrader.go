package mount

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/filecoin-project/dagstore/shard"
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
	shardKey    shard.Key

	lk      sync.Mutex
	refs    uint32
	rootdir string
}

var _ Mount = (*Upgrader)(nil)

// Upgrade constructs a new Upgrader for the underlying Mount. If provided, it
// will reuse the file in path `initial` as the initial transient copy. Whenever
// a new transient copy has to be created, it will be created under `rootdir`.
func Upgrade(underlying Mount, rootdir string, shardKey shard.Key, initial string) (u *Upgrader, finalErr error) {
	ret := &Upgrader{underlying: underlying, rootdir: rootdir, shardKey: shardKey}
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

	if _, err := os.Stat(ret.transientFilePath()); err == nil {
		return ret, nil
	}
	// clean up the existing transient if it's gone bad
	_ = os.Remove(ret.transientFilePath())

	if initial == "" {
		return ret, nil
	}
	// we don't want to rename the file given by the client -> simply copy it and use the upgrader's file naming scheme here.
	src, err := os.Open(initial)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	dst, err := os.Create(ret.transientFilePath())
	if err != nil {
		return nil, err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		_ = os.Remove(dst.Name())
		return nil, err
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

	if _, err := os.Stat(u.transientFilePath()); err == nil {
		return u.toTransientReaderCloserUnlocked()
	}

	// transient appears to be dead, refetch.
	if err := u.refetch(ctx); err != nil {
		return nil, err
	}

	return u.toTransientReaderCloserUnlocked()
}

func (u *Upgrader) Info() Info {
	return Info{
		Kind:             KindLocal,
		AccessSequential: true,
		AccessSeek:       true,
		AccessRandom:     true,
	}
}

func (u *Upgrader) Stat(ctx context.Context) (Stat, error) {

	if stat, err := os.Stat(u.transientFilePath()); err == nil {
		ret := Stat{Exists: true, Size: stat.Size()}
		return ret, nil
	}

	return u.underlying.Stat(ctx)
}

func (u *Upgrader) Serialize() *url.URL {
	return u.underlying.Serialize()
}

func (u *Upgrader) Deserialize(url *url.URL) error {
	return u.underlying.Deserialize(url)
}

func (u *Upgrader) Close() error {
	panic("implement me")
}

func (u *Upgrader) refetch(ctx context.Context) error {
	_ = os.Remove(u.transientFilePath())

	file, err := os.Create(u.transientFilePath())
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer file.Close()

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

func (u *Upgrader) toTransientReaderCloserUnlocked() (*transientReaderCloser, error) {
	f, err := os.Open(u.transientFilePath())
	if err != nil {
		return nil, err
	}
	u.refs++

	return &transientReaderCloser{
		Reader: f,
		u:      u,
	}, nil
}

func (u *Upgrader) transientFilePath() string {
	return filepath.Join(u.rootdir, u.shardKey.String())
}

type transientReaderCloser struct {
	Reader
	u *Upgrader
}

func (m *transientReaderCloser) Close() error {
	m.u.lk.Lock()
	defer m.u.lk.Unlock()

	m.u.refs--
	if err := m.Reader.Close(); err != nil {
		return err
	}

	// TODO Smarter transient management and GC in the future.
	if m.u.refs == 0 {
		err := os.Remove(m.u.transientFilePath())
		if err != nil {
			return err
		}

		return nil
	}

	return nil
}

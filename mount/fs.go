package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
)

const path = "path"

// FSMount is a mount that opens the file indicated by Path, using the
// provided fs.FS. Given that io/fs does not support random access patterns,
// this mount requires an Upgrade. It is suitable for testing.
type FSMount struct {
	FS   fs.FS
	Path string
}

var _ Mount = (*FSMount)(nil)

func (f *FSMount) Close() error {
	return nil // TODO
}

func (f *FSMount) Fetch(ctx context.Context) (Reader, error) {
	// yield if the context is cancelled.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	file, err := f.FS.Open(f.Path)
	if err != nil {
		return nil, err
	}
	ra, _ := file.(io.ReaderAt)
	sk, _ := file.(io.Seeker)
	return &fsReader{
		File:     file,
		ReaderAt: ra,
		Seeker:   sk,
	}, err
}

func (f *FSMount) Info() Info {
	return Info{
		Kind:             KindLocal,
		AccessSequential: true,
		AccessSeek:       false, // TODO actual capabilities depend on the fs.FS implementation!
		AccessRandom:     false, // TODO actual capabilities depend on the fs.FS implementation!
	}
}

func (f *FSMount) Stat(_ context.Context) (Stat, error) {
	st, err := fs.Stat(f.FS, f.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Stat{Exists: false, Size: 0}, nil
	}
	if err != nil {
		return Stat{}, err
	}
	return Stat{
		Exists: true,
		Size:   st.Size(),
	}, nil
}

func (f *FSMount) Serialize() *url.URL {
	u := new(url.URL)
	if st, err := fs.Stat(f.FS, f.Path); err != nil {
		u.Host = "irrecoverable"
	} else {
		q := u.Query()
		q.Set(path, f.Path)
		u.RawQuery = q.Encode()
		u.Host = st.Name()
	}
	return u
}

func (f *FSMount) Deserialize(u *url.URL) error {
	if u.Host == "irrecoverable" || u.Host == "" {
		return fmt.Errorf("invalid host")
	}

	f.Path = u.Query().Get(path)
	return nil
}

type fsReader struct {
	fs.File
	io.ReaderAt
	io.Seeker
}

var _ Reader = (*fsReader)(nil)

func (f *fsReader) ReadAt(p []byte, off int64) (n int, err error) {
	if f.ReaderAt == nil {
		return 0, ErrRandomAccessUnsupported
	}
	return f.ReaderAt.ReadAt(p, off)
}

func (f *fsReader) Seek(off int64, whence int) (int64, error) {
	if f.Seeker == nil {
		return 0, ErrSeekUnsupported
	}
	return f.Seeker.Seek(off, whence)
}

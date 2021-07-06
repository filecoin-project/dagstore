package mount

import (
	"context"
	"errors"
	"io/fs"
	"net/url"
)

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

func (f *FSMount) Fetch(_ context.Context) (Reader, error) {
	file, err := f.FS.Open(f.Path)
	return &fsReader{File: file}, err
}

func (f *FSMount) Info() Info {
	u := &url.URL{Scheme: "fs"}

	if st, err := fs.Stat(f.FS, f.Path); err != nil {
		u.Host = "irrecoverable"
	} else {
		u.Host = st.Name()
	}
	return Info{
		Kind:             KindLocal,
		URL:              u,
		AccessSequential: true,
		AccessSeek:       false,
		AccessRandom:     false,
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

type fsReader struct {
	fs.File
}

var _ Reader = (*fsReader)(nil)

func (f *fsReader) ReadAt(_ []byte, _ int64) (n int, err error) {
	return 0, ErrRandomAccessUnsupported
}

func (f *fsReader) Seek(_ int64, _ int) (int64, error) {
	return 0, ErrSeekUnsupported
}

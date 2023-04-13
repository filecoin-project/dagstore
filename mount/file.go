package mount

import (
	"context"
	"fmt"
	"net/url"
	"os"
)

type FileMount struct {
	Path string
}

var _ Mount = (*FileMount)(nil)

func (f *FileMount) Fetch(_ context.Context) (Reader, error) {
	return os.Open(f.Path)
}

func (f *FileMount) Info() Info {
	return Info{
		Kind:             KindLocal,
		AccessRandom:     true,
		AccessSeek:       true,
		AccessSequential: true,
	}
}

func (f *FileMount) Stat(_ context.Context) (Stat, error) {
	stat, err := os.Stat(f.Path)
	if err != nil && os.IsNotExist(err) {
		return Stat{}, err
	}
	return Stat{
		Exists: !os.IsNotExist(err),
		Size:   stat.Size(),
	}, err
}

func (f *FileMount) Serialize() *url.URL {
	return &url.URL{
		Path: f.Path,
	}
}

func (f *FileMount) Deserialize(u *url.URL) error {
	if u.Path == "" {
		return fmt.Errorf("invalid path")
	}
	f.Path = u.Path
	return nil
}

func (f *FileMount) Close() error {
	return nil
}

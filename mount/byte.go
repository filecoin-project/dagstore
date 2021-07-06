package mount

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/url"
)

// BytesMount encloses a byte slice. It is mainly used for testing. The
// Upgrader passes through it.
type BytesMount struct {
	Bytes []byte
}

var _ Mount = (*BytesMount)(nil)

func (b *BytesMount) Fetch(_ context.Context) (Reader, error) {
	r := bytes.NewReader(b.Bytes)
	return &NopCloser{
		Reader:   r,
		ReaderAt: r,
		Seeker:   r,
	}, nil
}

func (b *BytesMount) Info() Info {
	u := &url.URL{
		Scheme: "bytes",
		Host:   base64.StdEncoding.EncodeToString(b.Bytes),
	}
	return Info{
		Kind:             KindLocal,
		URL:              u,
		AccessSequential: true,
		AccessSeek:       true,
		AccessRandom:     true,
	}
}

func (b *BytesMount) Stat(_ context.Context) (Stat, error) {
	return Stat{
		Exists: true,
		Size:   int64(len(b.Bytes)),
	}, nil
}

func (b *BytesMount) Close() error {
	b.Bytes = nil // release
	return nil
}

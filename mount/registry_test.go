package mount

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ Mount = (*MockMount)(nil)

type MockMount struct {
	Val      string
	URL      *url.URL
	StatSize uint64
}

func (m *MockMount) FetchSeek(ctx context.Context) (io.ReadSeekCloser, error) {
	panic("implement me")
}

func (m *MockMount) Fetch(_ context.Context) (io.ReadCloser, error) {
	r := io.NopCloser(strings.NewReader(m.Val))
	return r, nil
}

func (m *MockMount) Info() Info {
	return Info{
		Source: SourceRemote,
		URL:    m.URL,
	}
}

func (m *MockMount) Stat() (Stat, error) {
	return Stat{
		Exists: true,
		Size:   m.StatSize,
	}, nil
}

type MockMountFactory1 struct{}

func (mf *MockMountFactory1) Parse(u *url.URL) (Mount, error) {
	vals, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	statSize, err := strconv.ParseUint(vals["size"][0], 10, 64)
	if err != nil {
		return nil, err
	}

	return &MockMount{
		Val:      u.Host,
		URL:      u,
		StatSize: statSize,
	}, nil
}

type MockMountFactory2 struct{}

func (mf *MockMountFactory2) Parse(u *url.URL) (Mount, error) {
	vals, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	statSize, err := strconv.ParseUint(vals["size"][0], 10, 64)
	if err != nil {
		return nil, err
	}

	return &MockMount{
		Val:      u.Host,
		URL:      u,
		StatSize: statSize * 2,
	}, nil
}

func TestRegistry(t *testing.T) {
	m1StatSize := uint64(1234)
	m2StatSize := uint64(5678)

	// create a registry
	r := Registry{
		m: make(map[string]Type),
	}

	// create & register mock mount factory 1
	u := fmt.Sprintf("http://host1:123?size=%d", m1StatSize)
	url, err := url.Parse(u)
	require.NoError(t, err)
	m1 := &MockMountFactory1{}
	require.NoError(t, r.Register("http", m1))
	// // register same scheme again -> fails
	require.Error(t, r.Register("http", m1))

	// create and register mock mount factory 2
	url2 := fmt.Sprintf("ftp://host2:1234?size=%d", m2StatSize)
	u2, err := url.Parse(url2)
	require.NoError(t, err)
	m2 := &MockMountFactory2{}
	require.NoError(t, r.Register("ftp", m2))

	// instantiate mount 1 and verify state is constructed correctly
	m, err := r.Instantiate(url)
	require.NoError(t, err)
	require.Equal(t, url.Host, fetchAndReadAll(t, m))
	stat, err := m.Stat()
	require.NoError(t, err)
	require.Equal(t, m1StatSize, stat.Size)

	// instantiate mount 2 and verify state is constructed correctly
	m, err = r.Instantiate(u2)
	require.NoError(t, err)
	require.Equal(t, u2.Host, fetchAndReadAll(t, m))
	stat, err = m.Stat()
	require.NoError(t, err)
	require.Equal(t, m2StatSize*2, stat.Size)
}

func fetchAndReadAll(t *testing.T, m Mount) string {
	rd, err := m.Fetch(context.Background())
	require.NoError(t, err)
	bz, err := ioutil.ReadAll(rd)
	require.NoError(t, err)
	return string(bz)
}

package mount

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"reflect"
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

func newMockMount(t *testing.T, u string, stateSize uint64) *MockMount {
	url, err := url.Parse(u)
	require.NoError(t, err)
	return &MockMount{
		URL:      url,
		Val:      url.Host,
		StatSize: stateSize,
	}
}

func (m *MockMount) Fetch(_ context.Context) (io.ReadCloser, error) {
	r := io.NopCloser(strings.NewReader(m.Val))
	return r, nil
}

func (m *MockMount) Info() Info {
	return Info{
		Kind: MountKindRemote,
		URL:  m.URL,
	}
}

func (m *MockMount) Stat() (Stat, error) {
	return Stat{
		Exists: true,
		Size:   m.StatSize,
	}, nil
}

func (m *MockMount) Parse(u *url.URL) error {
	m.Val = u.Host
	m.URL = u

	vals, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}

	m.StatSize, err = strconv.ParseUint(vals["size"][0], 10, 64)
	if err != nil {
		return err
	}

	return nil
}

func TestRegistry(t *testing.T) {
	m1StatSize := uint64(1234)
	m2StatSize := uint64(5678)

	// create a registry
	r := Registry{
		m: make(map[string]reflect.Type),
	}

	// create mock mount 1
	url := fmt.Sprintf("http://host1:123?size=%d", m1StatSize)
	m1 := newMockMount(t, url, m1StatSize)
	require.NoError(t, r.Register(m1.URL.Scheme, m1))
	// // register same scheme again -> fails
	require.Error(t, r.Register(m1.URL.Scheme, m1))

	// create and register mock mount 2
	url2 := fmt.Sprintf("ftp://host2:1234?size=%d", m2StatSize)
	m2 := newMockMount(t, url2, m2StatSize)
	require.NoError(t, r.Register(m2.URL.Scheme, m2))

	// instantiate mount 1 and verify state is constructed correctly
	m, err := r.Instantiate(m1.URL)
	require.NoError(t, err)
	require.Equal(t, m1.URL.Host, fetchAndReadAll(t, m))
	stat, err := m.Stat()
	require.NoError(t, err)
	require.Equal(t, m1StatSize, stat.Size)

	// instantiate mount 2 and verify state is constructed correctly
	m, err = r.Instantiate(m2.URL)
	require.NoError(t, err)
	require.Equal(t, m2.URL.Host, fetchAndReadAll(t, m2))
	stat, err = m.Stat()
	require.NoError(t, err)
	require.Equal(t, m2StatSize, stat.Size)
}

func TestRegistrationFailWithNonPointer(t *testing.T) {
	type NonPointerMount struct {
		Mount
	}

	// create a registry
	r := Registry{
		m: make(map[string]reflect.Type),
	}

	require.EqualError(t, r.Register("http", NonPointerMount{}), ErrRegistrationWithNonPointerType.Error())
}

func fetchAndReadAll(t *testing.T, m Mount) string {
	rd, err := m.Fetch(context.Background())
	require.NoError(t, err)
	bz, err := ioutil.ReadAll(rd)
	require.NoError(t, err)
	return string(bz)
}

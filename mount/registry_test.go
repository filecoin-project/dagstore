package mount

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ Mount = (*MockMount)(nil)

type MockMount struct {
	Val       string
	URL       *url.URL
	StatSize  int64
	Templated string
}

func (m *MockMount) Serialize() *url.URL {
	u := &url.URL{
		Scheme: "aaa", // random, will get replaced
		Host:   m.Val,
	}
	u.Query().Set("size", strconv.FormatInt(m.StatSize, 10))
	return u
}

func (m *MockMount) Deserialize(u *url.URL) error {
	vals, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}

	statSize, err := strconv.ParseInt(vals["size"][0], 10, 64)
	if err != nil {
		return err
	}

	if v, err := strconv.ParseBool(vals["timestwo"][0]); err != nil {
		return err
	} else if v {
		statSize *= 2
	}

	m.Val = u.Host
	m.URL = u
	m.StatSize = statSize
	return nil
}

func (m *MockMount) Close() error {
	panic("implement me")
}

func (m *MockMount) Fetch(_ context.Context) (Reader, error) {
	r := strings.NewReader(m.Val)
	return &NopCloser{Reader: r, ReaderAt: r, Seeker: r}, nil
}

func (m *MockMount) Info() Info {
	return Info{Kind: KindRemote}
}

func (m *MockMount) Stat(_ context.Context) (Stat, error) {
	return Stat{
		Exists: true,
		Size:   m.StatSize,
	}, nil
}

func TestRegistry(t *testing.T) {
	m1StatSize := uint64(1234)
	m2StatSize := uint64(5678)

	// create a registry
	r := NewRegistry()

	type (
		MockMount1 struct{ MockMount }
		MockMount2 struct{ MockMount }
		MockMount3 struct{ MockMount }
	)

	// create & register mock mount factory 1
	url1 := fmt.Sprintf("http://host1:123?size=%d&timestwo=false", m1StatSize)
	u1, err := url.Parse(url1)
	require.NoError(t, err)
	require.NoError(t, r.Register("http", new(MockMount1)))
	// register same scheme again -> fails
	require.Error(t, r.Register("http", new(MockMount2)))
	// register same type again -> fails, different scheme
	require.Error(t, r.Register("http2", new(MockMount1)))

	// create and register mock mount factory 2
	url2 := fmt.Sprintf("ftp://host2:1234?size=%d&timestwo=true", m2StatSize)
	u2, err := u1.Parse(url2)
	require.NoError(t, err)
	require.NoError(t, r.Register("ftp", new(MockMount3)))

	// instantiate mount 1 and verify state is constructed correctly
	m, err := r.Instantiate(u1)
	require.NoError(t, err)
	require.Equal(t, u1.Host, fetchAndReadAll(t, m))
	stat, err := m.Stat(context.TODO())
	require.NoError(t, err)
	require.EqualValues(t, m1StatSize, stat.Size)

	// instantiate mount 2 and verify state is constructed correctly
	m, err = r.Instantiate(u2)
	require.NoError(t, err)
	require.Equal(t, u2.Host, fetchAndReadAll(t, m))
	stat, err = m.Stat(context.TODO())
	require.NoError(t, err)
	require.EqualValues(t, m2StatSize*2, stat.Size)
}

func TestRegistryHonoursTemplate(t *testing.T) {
	r := NewRegistry()

	template := &MockMount{Templated: "give me proof"}
	err := r.Register("foo", template)
	require.NoError(t, err)

	u, err := url.Parse("foo://bang?size=100&timestwo=false")
	require.NoError(t, err)

	m, err := r.Instantiate(u)
	require.NoError(t, err)

	require.Equal(t, "give me proof", m.(*MockMount).Templated)
}

func TestRegistryRecognizedType(t *testing.T) {
	type (
		MockMount1 struct{ MockMount }
		MockMount2 struct{ MockMount }
		MockMount3 struct{ MockMount }
	)

	// register all three types under different schemes
	r := NewRegistry()
	err := r.Register("mount1", new(MockMount1))
	require.NoError(t, err)
	err = r.Register("mount2", new(MockMount2))
	require.NoError(t, err)
	err = r.Register("mount3", new(MockMount3))
	require.NoError(t, err)

	// now attempt to encode an instance of MockMount2
	u, err := r.Represent(&MockMount2{})
	require.NoError(t, err)

	require.Equal(t, "mount2", u.Scheme)
}

func fetchAndReadAll(t *testing.T, m Mount) string {
	rd, err := m.Fetch(context.Background())
	require.NoError(t, err)
	bz, err := ioutil.ReadAll(rd)
	require.NoError(t, err)
	return string(bz)
}

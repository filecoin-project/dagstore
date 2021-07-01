package index

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
)

// MockFullIndex implements an in-memory index that can used in tests
type MockFullIndex struct {
	lk  sync.RWMutex
	idx map[string]uint64
}

var _ FullIndex = (*MockFullIndex)(nil)

func NewMockFullIndex() *MockFullIndex {
	return &MockFullIndex{
		idx: make(map[string]uint64),
	}
}

func (m *MockFullIndex) Set(c cid.Cid, offset uint64) {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.idx[c.String()] = offset
}

func (m *MockFullIndex) Offset(c cid.Cid) (uint64, error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	offset, ok := m.idx[c.String()]
	if !ok {
		return 0, ErrNotFound
	}
	return offset, nil
}

func (m *MockFullIndex) Contains(c cid.Cid) (bool, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	_, ok := m.idx[c.String()]
	return ok, nil
}

func (m *MockFullIndex) Len() (l int64, err error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	return int64(len(m.idx)), nil
}

func (m *MockFullIndex) ForEach(f func(c cid.Cid, offset uint64) (ok bool, err error)) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	for str, offset := range m.idx {
		c, err := cid.Parse(str)
		if err != nil {
			return err
		}
		ok, err := f(c, offset)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}

	return nil
}

func (m *MockFullIndex) Marshal(w io.Writer) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	bs, err := json.Marshal(m.idx)
	if err != nil {
		return err
	}

	_, err = w.Write(bs)
	return err
}

func (m *MockFullIndex) Unmarshal(r io.Reader) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	bz, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(bz, &m.idx)
}

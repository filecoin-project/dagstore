package test

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/filecoin-project/dagstore/index"
	"github.com/ipfs/go-cid"
)

type MockIndex struct {
	lk  sync.RWMutex
	idx map[string]int64
}

func NewMockIndex() *MockIndex {
	return &MockIndex{
		idx: make(map[string]int64),
	}
}

func (m *MockIndex) Set(c cid.Cid, offset int64) {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.idx[c.String()] = offset
}

func (m *MockIndex) Offset(c cid.Cid) (int64, error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	offset, ok := m.idx[c.String()]
	if !ok {
		return 0, index.ErrNotFound
	}
	return offset, nil
}

func (m *MockIndex) Contains(c cid.Cid) (bool, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	_, ok := m.idx[c.String()]
	return ok, nil
}

func (m *MockIndex) Len() (l int64, err error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	return int64(len(m.idx)), nil
}

func (m *MockIndex) ForEach(f func(c cid.Cid, offset int64) (ok bool, err error)) error {
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

func (m *MockIndex) Marshal(w io.Writer) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	bs, err := json.Marshal(m.idx)
	if err != nil {
		return err
	}

	_, err = w.Write(bs)
	return err
}

func (m *MockIndex) Unmarshal(r io.Reader) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	bz, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(bz, &m.idx)
}

var _ index.FullIndex = (*MockIndex)(nil)

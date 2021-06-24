package index

import (
	"bytes"
	"sync"

	"github.com/filecoin-project/dagstore/shard"
)

// MemIndexRepo implements FullIndexRepo with an in-memory map.
type MemIndexRepo struct {
	lk   sync.RWMutex
	idxs map[string]FullIndex
}

func NewMemoryRepo() *MemIndexRepo {
	return &MemIndexRepo{idxs: make(map[string]FullIndex)}
}

func (m *MemIndexRepo) GetFullIndex(key shard.Key) (idx FullIndex, err error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	idx, ok := m.idxs[string(key)]
	if !ok {
		return nil, ErrNotFound
	}

	return idx, nil
}

func (m *MemIndexRepo) AddFullIndex(key shard.Key, index FullIndex) (err error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.idxs[string(key)] = index

	return nil
}

func (m *MemIndexRepo) DropFullIndex(key shard.Key) (dropped bool, err error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	// TODO need to check if the index exists to be able to report whether it was dropped or not.
	delete(m.idxs, string(key))

	return true, nil
}

func (m *MemIndexRepo) StatFullIndex(key shard.Key) (Stat, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	idx, ok := m.idxs[string(key)]
	if !ok {
		return Stat{Exists: false}, nil
	}

	// Marshal the index just to get the size.
	// Could optimize by memoizing this although I don't think it's necessary
	// as the memory index repo is likely only used in tests.
	var buff bytes.Buffer
	err := idx.Marshal(&buff)
	if err != nil {
		return Stat{}, err
	}

	return Stat{
		Exists: ok,
		Size:   uint64(buff.Len()),
	}, nil
}

var _ FullIndexRepo = (*MemIndexRepo)(nil)

package index

import (
	"bytes"
	"sync"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipld/go-car/v2/index"
)

// MemIndexRepo implements FullIndexRepo with an in-memory map.
type MemIndexRepo struct {
	lk   sync.RWMutex
	idxs map[shard.Key]index.Index
}

func NewMemoryRepo() *MemIndexRepo {
	return &MemIndexRepo{idxs: make(map[shard.Key]index.Index)}
}

func (m *MemIndexRepo) GetFullIndex(key shard.Key) (idx index.Index, err error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	idx, ok := m.idxs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return idx, nil
}

func (m *MemIndexRepo) AddFullIndex(key shard.Key, index index.Index) (err error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.idxs[key] = index

	return nil
}

func (m *MemIndexRepo) DropFullIndex(key shard.Key) (dropped bool, err error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	// TODO need to check if the index exists to be able to report whether it was dropped or not.
	delete(m.idxs, key)

	return true, nil
}

func (m *MemIndexRepo) StatFullIndex(key shard.Key) (Stat, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	_, ok := m.idxs[key]
	if !ok {
		return Stat{Exists: false}, nil
	}

	size, err := m.indexSize(key)
	if err != nil {
		return Stat{}, err
	}

	return Stat{
		Exists: ok,
		Size:   size,
	}, nil
}

func (m *MemIndexRepo) ForEach(f func(shard.Key) (bool, error)) error {
	m.lk.RLock()
	ks := make([]shard.Key, 0, len(m.idxs))
	for k := range m.idxs {
		ks = append(ks, k)
	}
	m.lk.RUnlock()

	for _, k := range ks {
		ok, err := f(k)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

func (m *MemIndexRepo) Len() (int, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	return len(m.idxs), nil
}

func (m *MemIndexRepo) Size() (uint64, error) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	var size uint64
	for k := range m.idxs {
		k := shard.Key(k)
		sz, err := m.indexSize(k)
		if err != nil {
			return 0, err
		}
		size += sz
	}
	return size, nil
}

func (m *MemIndexRepo) indexSize(k shard.Key) (uint64, error) {
	idx, ok := m.idxs[k]
	if !ok {
		return 0, ErrNotFound
	}

	// Marshal the index just to get the size.
	// Could optimize by memoizing this although I don't think it's necessary
	// as the memory index repo is likely only used in tests.
	var buff bytes.Buffer
	_, err := index.WriteTo(idx, &buff)
	if err != nil {
		return 0, err
	}
	return uint64(buff.Len()), nil
}

var _ FullIndexRepo = (*MemIndexRepo)(nil)

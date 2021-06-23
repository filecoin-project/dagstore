package mem

import (
	"bytes"
	"sync"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/shard"
)

// MemIndexRepo implements FullIndexRepo with an in-memory map
type MemIndexRepo struct {
	lk   sync.RWMutex
	idxs map[string]index.FullIndex
}

func NewMemIndexRepo() *MemIndexRepo {
	return &MemIndexRepo{idxs: make(map[string]index.FullIndex)}
}

func (l *MemIndexRepo) GetFullIndex(key shard.Key) (idx index.FullIndex, err error) {
	l.lk.RLock()
	defer l.lk.RUnlock()

	idx, ok := l.idxs[string(key)]
	if !ok {
		return nil, index.ErrNotFound
	}

	return idx, nil
}

func (l *MemIndexRepo) AddFullIndex(key shard.Key, index index.FullIndex) (err error) {
	l.lk.Lock()
	defer l.lk.Unlock()

	l.idxs[string(key)] = index

	return nil
}

func (l *MemIndexRepo) DropFullIndex(key shard.Key) (dropped bool, err error) {
	l.lk.Lock()
	defer l.lk.Unlock()

	delete(l.idxs, string(key))

	return true, nil
}

func (l *MemIndexRepo) StatFullIndex(key shard.Key) (index.Stat, error) {
	l.lk.RLock()
	defer l.lk.RUnlock()

	idx, ok := l.idxs[string(key)]
	if !ok {
		return index.Stat{Exists: false}, nil
	}

	// Marshall the index just to get the size.
	// Could optimize by memoizing this although I don't think it's necessary
	// as the memory index repo is likely only used in tests.
	var buff bytes.Buffer
	err := idx.Marshal(&buff)
	if err != nil {
		return index.Stat{}, err
	}

	return index.Stat{
		Exists: ok,
		Size:   uint64(buff.Len()),
	}, nil
}

var _ index.FullIndexRepo = (*MemIndexRepo)(nil)

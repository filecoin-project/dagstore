package dagstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/filecoin-project/dagstore/shard"
)

// TODO WIP

type ScratchSpace struct {
	root string

	// salt is a string that's mixed into the filename when creating a new file.
	salt string
	// counter is an atomic counter to avoid filename collisions.
	counter int32

	lk    sync.Mutex
	files map[shard.Key]*scratchFile
}

type scratchFile struct {
	path string
	refs int
}

// NewScratchSpace
func NewScratchSpace(root string, entries map[shard.Key]string, salt string) (*ScratchSpace, error) {
	stat, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("failed to create scratch space: %w", err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("scratch space path is not a directory: %s", root)
	}

	m := make(map[shard.Key]*scratchFile, len(entries))
	for k, v := range entries {
		if stat, err := os.Stat(v); err != nil || stat.IsDir() {
			continue
		}
		m[k] = &scratchFile{path: v}
	}

	return &ScratchSpace{root: root, files: m, salt: salt}, nil
}

func (s *ScratchSpace) Acquire(key shard.Key) (path string, created bool, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	if f, ok := s.files[key]; ok {
		f.refs++
		return f.path, false, nil
	}

	c := atomic.AddInt32(&s.counter, 1)
	path = filepath.Join(s.root, fmt.Sprintf("%s-%s-%d.data", key.String(), s.salt, c))
	f, err := os.Create(path)
	if err != nil {
		return "", false, fmt.Errorf("failed to create scratch file: %w", err)
	}

	_ = f.Close()

	s.files[key] = &scratchFile{path: path, refs: 1}
	return path, true, nil
}

func (s *ScratchSpace) Release(key shard.Key) {
	s.lk.Lock()
	defer s.lk.Unlock()

	if f, ok := s.files[key]; ok {
		if f.refs <= 0 {
			panic("called ScratchSpace.Release illegally")
		}
		f.refs--
	}
}

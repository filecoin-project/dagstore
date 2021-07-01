package index

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/xerrors"

	"github.com/mr-tron/base58"

	"github.com/filecoin-project/dagstore/shard"
)

const (
	repoVersion = "1"
	indexSuffix = ".full.idx"
)

// FullIndexFactory provides a mechanism to load a FullIndex from a file path
type FullIndexFactory interface {
	// Build returns a FullIndex loaded from the given path
	Build(path string) (FullIndex, error)
}

// FSIndexRepo implements FullIndexRepo using the local file system to store
// the indices
type FSIndexRepo struct {
	baseDir string
}

// NewFSRepo creates a new index repo that stores indices on the local
// filesystem with the given base directory as the root
func NewFSRepo(baseDir string) (*FSIndexRepo, error) {
	err := os.MkdirAll(baseDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	l := &FSIndexRepo{baseDir: baseDir}

	// Get the repo version
	bs, err := os.ReadFile(l.versionPath())
	if err != nil {
		// If the repo has not been initialized, write out the repo version file
		if os.IsNotExist(err) {
			err = os.WriteFile(l.versionPath(), []byte(repoVersion), 0666)
			if err != nil {
				return nil, err
			}
			return l, nil
		}

		// There was some other error
		return nil, err
	}

	// Check that this library can read this repo
	if string(bs) != repoVersion {
		return nil, xerrors.Errorf("cannot read existing repo with version %s", bs)
	}

	return l, nil
}

func (l *FSIndexRepo) GetFullIndex(key shard.Key) (FullIndex, error) {
	// TODO: use a registry / factory instead of hard-coding CarFullIndex
	idx := &CarFullIndex{}
	path := l.indexPath(key)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	err = idx.Unmarshal(f)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (l *FSIndexRepo) AddFullIndex(key shard.Key, index FullIndex) (err error) {
	// Create a file at the key path
	f, err := os.Create(l.indexPath(key))
	if err != nil {
		return err
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// Write the index to the file
	return index.Marshal(f)
}

func (l *FSIndexRepo) DropFullIndex(key shard.Key) (dropped bool, err error) {
	// Remove the file at the key path
	return true, os.Remove(l.indexPath(key))
}

func (l *FSIndexRepo) StatFullIndex(key shard.Key) (Stat, error) {
	// Stat the file at the key path
	info, err := os.Stat(l.indexPath(key))
	if err != nil {
		// Check if the file exists
		if os.IsNotExist(err) {
			// Should we return ErrNotFound instead of Stat{Exists:false} ?
			return Stat{Exists: false}, nil
		}
		return Stat{}, err
	}

	return Stat{
		Exists: true,
		Size:   uint64(info.Size()),
	}, nil
}

var stopWalk = xerrors.New("stop walk")

// ForEach iterates over each index file to extract the key
func (l *FSIndexRepo) ForEach(f func(shard.Key) (bool, error)) error {
	// Iterate over each index file
	err := l.eachIndexFile(func(info os.FileInfo) error {
		// The file name is derived by base 58 encoding the key
		// so decode the file name to get the key
		name := info.Name()
		b58k := name[:len(name)-len(indexSuffix)]
		k, err := base58.Decode(b58k)
		if err != nil {
			return err
		}

		// Call the callback with the key
		ok, err := f(k)
		if err != nil {
			return err
		}
		if !ok {
			return stopWalk
		}
		return nil
	})
	if err == stopWalk {
		return nil
	}
	return err
}

// Len counts all index files in the base path
func (l *FSIndexRepo) Len() (int, error) {
	len := 0
	err := l.eachIndexFile(func(info os.FileInfo) error {
		len++
		return nil
	})
	return len, err
}

// Size sums the size of all index files in the base path
func (l *FSIndexRepo) Size() (uint64, error) {
	var size uint64
	err := l.eachIndexFile(func(info os.FileInfo) error {
		size += uint64(info.Size())
		return nil
	})
	return size, err
}

// eachIndexFile calls the callback for each index file
func (l *FSIndexRepo) eachIndexFile(f func(info os.FileInfo) error) error {
	return filepath.Walk(l.baseDir, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(info.Name(), indexSuffix) {
			return f(info)
		}
		return nil
	})
}

func (l *FSIndexRepo) indexPath(key shard.Key) string {
	return filepath.Join(l.baseDir, base58.Encode(key)+indexSuffix)
}

func (l *FSIndexRepo) versionPath() string {
	return filepath.Join(l.baseDir, ".version")
}

var _ FullIndexRepo = (*FSIndexRepo)(nil)

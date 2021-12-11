package index

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/filecoin-project/dagstore/shard"
	carindex "github.com/ipld/go-car/v2/index"

	"golang.org/x/xerrors"
)

const (
	repoVersion = "1"
	indexSuffix = ".full.idx"
)

// FSIndexRepo implements FullIndexRepo using the local file system to store
// the indices
type FSIndexRepo struct {
	baseDir string
}

var _ FullIndexRepo = (*FSIndexRepo)(nil)

// NewFSRepo creates a new index repo that stores indices on the local
// filesystem with the given base directory as the root
func NewFSRepo(baseDir string) (*FSIndexRepo, error) {
	err := os.MkdirAll(baseDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create index repo dir: %w", err)
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

func (l *FSIndexRepo) GetFullIndex(key shard.Key) (carindex.Index, error) {
	path := l.indexPath(key)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	return carindex.ReadFrom(f)
}

func (l *FSIndexRepo) AddFullIndex(key shard.Key, index carindex.Index) (err error) {
	// Create a file at the key path
	f, err := os.Create(l.indexPath(key))
	if err != nil {
		return err
	}
	defer f.Close()

	// Write the index to the file
	_, err = carindex.WriteTo(index, f)
	return err
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
		name = name[:len(name)-len(indexSuffix)]
		k := shard.KeyFromString(name)

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
	ret := 0
	err := l.eachIndexFile(func(info os.FileInfo) error {
		ret++
		return nil
	})
	return ret, err
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
	return filepath.Join(l.baseDir, key.String()+indexSuffix)
}

func (l *FSIndexRepo) versionPath() string {
	return filepath.Join(l.baseDir, ".version")
}

package local

import (
	"os"
	"path/filepath"

	"golang.org/x/xerrors"

	"github.com/mr-tron/base58"

	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/shard"
)

const repoVersion = "1"

// FullIndexFactory provides a mechanism to load a FullIndex from a file path
type FullIndexFactory interface {
	// Build returns a FullIndex loaded from the given path
	Build(path string) (index.FullIndex, error)
}

// LocalIndexRepo implements FullIndexRepo using the local file system to store
// the indices
type LocalIndexRepo struct {
	factory FullIndexFactory
	baseDir string
}

// NewLocalIndexRepo creates a new index repo that stores indices on the local
// filesystem with the given base directory as the root
func NewLocalIndexRepo(factory FullIndexFactory, baseDir string) (*LocalIndexRepo, error) {
	err := os.MkdirAll(baseDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	l := &LocalIndexRepo{factory: factory, baseDir: baseDir}

	bs, err := os.ReadFile(l.versionPath())
	fileNotFound := err != nil && os.IsNotExist(err)
	if fileNotFound {
		err = os.WriteFile(l.versionPath(), []byte(repoVersion), 0666)
		if err != nil {
			return nil, err
		}
		return l, nil
	}

	if string(bs) != repoVersion {
		return nil, xerrors.Errorf("cannot read existing repo with version %s", bs)
	}

	return l, nil
}

func (l *LocalIndexRepo) GetFullIndex(key shard.Key) (idx index.FullIndex, err error) {
	// Use the factory to build a FullIndex from the file at the key path
	return l.factory.Build(l.indexPath(key))
}

func (l *LocalIndexRepo) AddFullIndex(key shard.Key, index index.FullIndex) (err error) {
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

func (l *LocalIndexRepo) DropFullIndex(key shard.Key) (dropped bool, err error) {
	// Remove the file at the key path
	return true, os.Remove(l.indexPath(key))
}

func (l *LocalIndexRepo) StatFullIndex(key shard.Key) (index.Stat, error) {
	// Stat the file at the key path
	info, err := os.Stat(l.indexPath(key))
	if err != nil {
		// Check if the file exists
		if os.IsNotExist(err) {
			// Should we return ErrNotFound instead of Stat{Exists:false} ?
			return index.Stat{Exists: false}, nil
		}
		return index.Stat{}, err
	}

	return index.Stat{
		Exists: true,
		Size:   uint64(info.Size()),
	}, nil
}

func (l *LocalIndexRepo) indexPath(key shard.Key) string {
	return filepath.Join(l.baseDir, base58.Encode(key)+".full.idx")
}

func (l *LocalIndexRepo) versionPath() string {
	return filepath.Join(l.baseDir, ".version")
}

var _ index.FullIndexRepo = (*LocalIndexRepo)(nil)

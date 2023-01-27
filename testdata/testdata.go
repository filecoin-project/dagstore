package testdata

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
)

const (
	FSPathCarV1 = "files/sample-v1.car"
	FSPathCarV2 = "files/sample-wrapped-v2.car"
	FSPathJunk  = "files/junk.dat"

	RootPathCarV1 = "testdata/files/sample-v1.car"
	RootPathCarV2 = "testdata/files/sample-wrapped-v2.car"
	RootPathJunk  = "testdata/files/funk.dat"

	defaultHashFunction = uint64(multihash.BLAKE2B_MIN + 31)
	unixfsChunkSize     = uint64(1 << 10)
	unixfsLinksPerLevel = 1024
)

var (
	//go:embed files/*
	FS embed.FS

	CarV1 []byte
	CarV2 []byte
	Junk  []byte

	// RootCID is the root CID of the carv2 for testing.
	RootCID cid.Cid
)

func init() {
	var err error
	CarV1, err = FS.ReadFile(FSPathCarV1)
	if err != nil {
		panic(err)
	}

	CarV2, err = FS.ReadFile(FSPathCarV2)
	if err != nil {
		panic(err)
	}

	Junk, err = FS.ReadFile(FSPathJunk)
	if err != nil {
		panic(err)
	}

	reader, err := car.NewReader(bytes.NewReader(CarV2))
	if err != nil {
		panic(fmt.Errorf("failed to parse carv2: %w", err))
	}
	defer reader.Close()

	roots, err := reader.Roots()
	if err != nil {
		panic(fmt.Errorf("failed to obtain carv2 roots: %w", err))
	}
	if len(roots) == 0 {
		panic("carv2 has no roots")
	}
	RootCID = roots[0]
}

func CreateRandomFile(dir string, rseed, size int) (string, error) {
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))

	file, err := os.CreateTemp(dir, "sourcefile.dat")
	if err != nil {
		return "", err
	}

	_, err = io.Copy(file, source)
	if err != nil {
		return "", err
	}

	//
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}

// CreateDenseCARv2 generates a "dense" UnixFS CARv2 from the supplied ordinary file.
// A dense UnixFS CARv2 is one storing leaf data. Contrast to CreateRefCARv2.
func CreateDenseCARv2(dir, src string) (cid.Cid, string, error) {
	bs := bstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	dagSvc := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	root, err := WriteUnixfsDAGTo(src, dagSvc)
	if err != nil {
		return cid.Undef, "", err
	}

	// Create a UnixFS DAG again AND generate a CARv2 file using a CARv2
	// read-write blockstore now that we have the root.
	out, err := os.CreateTemp(dir, "rand")
	if err != nil {
		return cid.Undef, "", err
	}
	err = out.Close()
	if err != nil {
		return cid.Undef, "", err
	}

	rw, err := blockstore.OpenReadWrite(out.Name(), []cid.Cid{root}, blockstore.UseWholeCIDs(true))
	if err != nil {
		return cid.Undef, "", err
	}

	dagSvc = merkledag.NewDAGService(blockservice.New(rw, offline.Exchange(rw)))

	root2, err := WriteUnixfsDAGTo(src, dagSvc)
	if err != nil {
		return cid.Undef, "", err
	}

	err = rw.Finalize()
	if err != nil {
		return cid.Undef, "", err
	}

	if root != root2 {
		return cid.Undef, "", fmt.Errorf("DAG root cid mismatch")
	}

	return root, out.Name(), nil
}

func WriteUnixfsDAGTo(path string, into ipldformat.DAGService) (cid.Cid, error) {
	file, err := os.Open(path)
	if err != nil {
		return cid.Undef, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return cid.Undef, err
	}

	// get a IPLD reader path file
	// required to write the Unixfs DAG blocks to a filestore
	rpf, err := files.NewReaderPathFile(file.Name(), file, stat)
	if err != nil {
		return cid.Undef, err
	}

	// generate the dag and get the root
	// import to UnixFS
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, err
	}

	prefix.MhType = defaultHashFunction

	bufferedDS := ipldformat.NewBufferedDAG(context.Background(), into)
	params := ihelper.DagBuilderParams{
		Maxlinks:  unixfsLinksPerLevel,
		RawLeaves: true,
		// NOTE: InlineBuilder not recommended, we are using this to test identity CIDs
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   126,
		},
		Dagserv: bufferedDS,
		NoCopy:  true,
	}

	db, err := params.New(chunk.NewSizeSplitter(rpf, int64(unixfsChunkSize)))
	if err != nil {
		return cid.Undef, err
	}

	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	err = bufferedDS.Commit()
	if err != nil {
		return cid.Undef, err
	}

	err = rpf.Close()
	if err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}

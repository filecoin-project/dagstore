module github.com/celestiaorg/dagstore

go 1.16

require (
	github.com/filecoin-project/dagstore v0.0.0-00010101000000-000000000000
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.5.0
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-ipfs-blocksutil v0.0.1
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/ipld/go-car/v2 v2.1.1
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multicodec v0.3.1-0.20210902112759-1539a079fd61
	github.com/multiformats/go-multihash v0.1.0
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/goleveldb v1.0.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20200123233031-1cdf64d27158
	golang.org/x/exp v0.0.0-20210714144626-1041f73d31d8
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

replace github.com/filecoin-project/dagstore => ./

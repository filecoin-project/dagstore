module github.com/celestiaorg/dagstore

go 1.16

require (
	github.com/filecoin-project/dagstore v0.0.0-00010101000000-000000000000
	github.com/ipfs/go-block-format v0.1.2
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-cidutil v0.1.0 // indirect
	github.com/ipfs/go-datastore v0.6.0
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-ipfs-blockstore v1.3.0
	github.com/ipfs/go-ipfs-blocksutil v0.0.1
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipld/go-car/v2 v2.10.1
	github.com/jellydator/ttlcache/v2 v2.11.1
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multicodec v0.9.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/stretchr/testify v1.8.4
	github.com/syndtr/goleveldb v1.0.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20230126041949-52956bd4c9aa
	golang.org/x/exp v0.0.0-20230213192124-5e25df0256eb
	golang.org/x/sync v0.1.0
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
)

replace github.com/filecoin-project/dagstore => ./

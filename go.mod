module github.com/filecoin-project/dagstore

go 1.16

require (
	github.com/filecoin-project/lotus v0.0.0-00010101000000-000000000000
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-leveldb v0.4.2 // indirect
	github.com/ipfs/go-ds-measure v0.1.0 // indirect
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/ipld/go-car/v2 v2.0.3-0.20210811121346-c514a30114d7
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multicodec v0.3.0
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8
	golang.org/x/exp v0.0.0-20210715201039-d37aa40e8013
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

replace github.com/filecoin-project/lotus => ../lotus

replace github.com/filecoin-project/filecoin-ffi => ../lotus/extern/filecoin-ffi

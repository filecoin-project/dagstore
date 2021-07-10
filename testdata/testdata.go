package testdata

import (
	"bytes"
	"embed"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

const (
	FSPathCarV1 = "files/sample-v1.car"
	FSPathCarV2 = "files/sample-wrapped-v2.car"

	RootPathCarV1 = "testdata/files/sample-v1.car"
	RootPathCarV2 = "testdata/files/sample-wrapped-v2.car"
)

var (
	//go:embed files/*
	FS embed.FS

	CarV1 []byte
	CarV2 []byte

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

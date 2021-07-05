package main

import (
	"fmt"
	"os"

	"github.com/filecoin-project/dagstore"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("./shard_gen.go", "dagstore",
		dagstore.PersistedShard{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

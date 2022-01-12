package shard

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyMarshalJSON(t *testing.T) {
	k := Key{"abc"}
	bz, err := json.Marshal(k)
	require.NoError(t, err)

	var k2 Key
	err = json.Unmarshal(bz, &k2)
	require.NoError(t, err)

	require.Equal(t, k.str, k2.str)
}

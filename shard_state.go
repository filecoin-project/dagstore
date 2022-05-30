package dagstore

type ShardState byte

const (
	// ShardStateNew indicates that a shard has just been registered and is
	// about to be processed for activation.
	ShardStateNew ShardState = iota

	// ShardStateAvailable indicates that the shard has been initialized and is
	// active for serving queries.
	ShardStateAvailable

	// ShardStateErrored indicates that an unexpected error was encountered
	// during a shard operation, and therefore the shard needs to be recovered.
	ShardStateErrored ShardState = 0xf0

	// ShardStateUnknown indicates that it's not possible to determine the state
	// of the shard. This state is currently unused, but it's reserved.
	ShardStateUnknown ShardState = 0xff
)

func (ss ShardState) String() string {
	strs := [...]string{
		ShardStateNew:       "ShardStateNew",
		ShardStateAvailable: "ShardStateAvailable",
		ShardStateErrored:   "ShardStateErrored",
		ShardStateUnknown:   "ShardStateUnknown",
	}
	if ss < 0 || int(ss) >= len(strs) {
		// safety comes first.
		return "__undefined__"
	}
	return strs[ss]
}

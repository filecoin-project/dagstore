package dagstore

type OpType int

const (
	OpShardRegister OpType = iota
	OpShardFetch
	OpShardFetchDone
	OpShardIndex
	OpShardIndexDone
	OpShardMakeAvailable
	OpShardDestroy
	OpShardAcquire
	OpShardFail
	OpShardRelease
)

func (o OpType) String() string {
	return [...]string{
		"OpShardRegister",
		"OpShardFetch",
		"OpShardFetchDone",
		"OpShardIndex",
		"OpShardIndexDone",
		"OpShardMakeAvailable",
		"OpShardDestroy",
		"OpShardAcquire",
		"OpShardFail",
		"OpShardRelease"}[o]
}

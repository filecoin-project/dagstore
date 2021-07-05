package dagstore

type OpType int

const (
	OpShardRegister OpType = iota
	OpShardMakeAvailable
	OpShardDestroy
	OpShardAcquire
	OpShardFail
	OpShardRelease
)

func (o OpType) String() string {
	return [...]string{
		"OpShardRegister",
		"OpShardMakeAvailable",
		"OpShardDestroy",
		"OpShardAcquire",
		"OpShardFail",
		"OpShardRelease"}[o]
}

package mapper

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

const (
	// MapNoopFn represents a no-op map function.
	MapNoopFn = "Noop"
)

// MapNoop does nothing.
func MapNoop(key string, value []byte) []*mrutil.KeyValue {
	return []*mrutil.KeyValue{&mrutil.KeyValue{Key: key, Value: string(value)}}
}

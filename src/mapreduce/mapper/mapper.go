package mapper

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

// MapFunc represents the mapping function in a MapReduce process.
type MapFunc = func(chunk *mrutil.DataChunk) error

// Mapper represents a Map performer.
type Mapper interface {
	Map(chunk *mrutil.DataChunk) error
}

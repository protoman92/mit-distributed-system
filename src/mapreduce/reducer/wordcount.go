package reducer

import (
	"strconv"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

const (
	// ReduceSumFn is the name for the ReduceSum function.
	ReduceSumFn mrutil.ReduceFuncName = "ReduceSum"
)

// ReduceSum counts the number of occurrences of a key.
func ReduceSum(key string, values []string) mrutil.KeyValue {
	sum := int64(0)

	for ix := range values {
		if conv, err := strconv.ParseInt(values[ix], 10, 0); err == nil {
			sum += conv
		}
	}

	return mrutil.KeyValue{Key: key, Value: strconv.FormatInt(sum, 10)}
}

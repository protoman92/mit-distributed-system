package reducer

import (
	"github.com/protoman92/gocompose/pkg"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccess"
)

// Params represents the required parameters to build a Reducer.
type Params struct {
	FileAccessor   fileaccess.FileAccessor
	RetryWithDelay compose.ErrorTransformFunc
}

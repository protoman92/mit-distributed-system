package localMapper

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

func (lm *localMapper) Map(chunk *mrutil.DataChunk) error {
	return lm.MapFunc(chunk)
}

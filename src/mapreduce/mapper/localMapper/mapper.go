package localMapper

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"

// Params represents the required parameters to build a local Mapper.
type Params struct {
	MapFunc mapper.MapFunc
}

func checkParams(params *Params) *Params {
	if params.MapFunc == nil {
		panic("Invalid parameters")
	}

	return params
}

type localMapper struct {
	*Params
}

// NewLocalMapper returns a new LocalMapper.
func NewLocalMapper(params Params) mapper.Mapper {
	checked := checkParams(&params)
	lm := &localMapper{Params: checked}
	return lm
}

package string

import (
	"github.com/protoman92/mit-distributed-system/src/util"

	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
)

// Params represents the required parameters to build a StringSplitter.
type Params struct {
	ChunkCount uint
	LogMan     util.LogMan
	SplitToken byte
}

func checkParams(params *Params) *Params {
	if params.ChunkCount == 0 || params.SplitToken == 0 {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{})
	}

	return params
}

type stringSplitter struct {
	*Params
}

// NewStringSplitter returns a new StringSplitter.
func NewStringSplitter(params Params) sp.Splitter {
	checked := checkParams(&params)
	splitter := &stringSplitter{Params: checked}
	return splitter
}

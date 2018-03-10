package local

import (
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
	ss "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter/string"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

// Params represents the required parameters to build a LocalInputReader.
type Params struct {
	SplitterParams ss.Params
	FilePaths      []string
}

func checkParams(params *Params) *Params {
	if params.FilePaths == nil || len(params.FilePaths) == 0 {
		panic("Invalid parameters")
	}

	return params
}

type localInputReader struct {
	*Params
	sp.Splitter
	errCh  chan error
	readCh chan *util.KeyValueChunk
}

// NewLocalInputReader returns a new LocalInputReader.
func NewLocalInputReader(params Params) ir.InputReader {
	checked := checkParams(&params)

	lr := &localInputReader{
		Params:   checked,
		Splitter: ss.NewStringSplitter(checked.SplitterParams),
		errCh:    make(chan error, 1),
		readCh:   make(chan *util.KeyValueChunk, 0),
	}

	for ix := range params.FilePaths {
		go lr.readFileAtPath(params.FilePaths[ix])
	}

	return lr
}

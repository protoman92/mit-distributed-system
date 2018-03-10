package localReader

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/splitter/stringSplitter"
)

// Params represents the required parameters to build a LocalInputReader.
type Params struct {
	SplitterParams stringSplitter.Params
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
	splitter.Splitter
	errCh  chan error
	readCh chan *mrutil.DataChunk
}

// NewLocalInputReader returns a new LocalInputReader.
func NewLocalInputReader(params Params) inputReader.InputReader {
	checked := checkParams(&params)

	lr := &localInputReader{
		Params:   checked,
		Splitter: stringSplitter.NewStringSplitter(checked.SplitterParams),
		errCh:    make(chan error, 1),
		readCh:   make(chan *mrutil.DataChunk, 0),
	}

	for ix := range params.FilePaths {
		go lr.readFileAtPath(params.FilePaths[ix])
	}

	return lr
}

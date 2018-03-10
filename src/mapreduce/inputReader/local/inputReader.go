package local

import (
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

// Params represents the required parameters to build a LocalInputReader.
type Params struct {
	FilePaths []string
}

type localInputReader struct {
	*Params
	errCh  chan error
	readCh chan *util.KeyValuePipe
}

func (lr *localInputReader) ErrorChannel() <-chan error {
	return lr.errCh
}

func (lr *localInputReader) ReadInputChannel() <-chan *util.KeyValuePipe {
	return lr.readCh
}

// NewLocalInputReader returns a new LocalInputReader.
func NewLocalInputReader(params Params) ir.InputReader {
	lr := &localInputReader{
		Params: &params,
		errCh:  make(chan error, 1),
		readCh: make(chan *util.KeyValuePipe, 0),
	}

	for ix := range params.FilePaths {
		go lr.loopWork(params.FilePaths[ix])
	}

	return lr
}

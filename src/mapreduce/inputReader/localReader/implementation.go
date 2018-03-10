package localReader

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

func (lr *localInputReader) ErrorChannel() <-chan error {
	return lr.errCh
}

func (lr *localInputReader) ReadInputChannel() <-chan *mrutil.DataChunk {
	return lr.readCh
}

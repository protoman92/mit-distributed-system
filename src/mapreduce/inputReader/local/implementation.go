package local

import "github.com/protoman92/mit-distributed-system/src/mapreduce/util"

func (lr *localInputReader) ErrorChannel() <-chan error {
	return lr.errCh
}

func (lr *localInputReader) ReadInputChannel() <-chan *util.KeyValueChunk {
	return lr.readCh
}

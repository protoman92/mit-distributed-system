package inputReader

import "github.com/protoman92/mit-distributed-system/src/mapreduce/util"

// InputReader represents a handler that reads a provided input for a MapReduce
// process. A simple implementation (for e.g. on a local machine) may only
// read from a local file.
type InputReader interface {
	DoneInputChannel() <-chan interface{}
	ErrorChannel() <-chan error
	ReadInputChannel() <-chan *util.KeyValuePipe
}

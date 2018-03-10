package inputReader

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

// InputReader represents a handler that reads a provided input for a MapReduce
// process. A simple implementation (for e.g. on a local machine) may only
// read from a local file and handle splitting as well.
type InputReader interface {
	ErrorChannel() <-chan error
	ReadInputChannel() <-chan *mrutil.DataChunk
}

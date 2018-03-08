package readWriter

// ReadWriter represents a handler that manages file input/output for a
// MapReduce process. A simple implementation (for e.g. on a local machine) may
// only read/write to local files.
type ReadWriter interface {
	DoneInputChannel() <-chan interface{}
	ErrorChannel() <-chan error
	ReadInputChannel() <-chan []byte
	TotalSizeChannel() <-chan uint64
}

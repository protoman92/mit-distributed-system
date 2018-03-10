package splitter

// Splitter is responsible for splitting some input into chunks.
type Splitter interface {
	// The parameters in this method cannot be supplied at creation time because
	// they are only available once an input is being read. The emission is a
	// slice of byte slices because we want the caller to deal with discrete
	// inputs (for e.g., join them together with newlines).
	SplitInput(inputCh <-chan []byte, totalSize int64) <-chan [][]byte
}

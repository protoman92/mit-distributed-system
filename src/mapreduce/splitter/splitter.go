package splitter

// Splitter is responsible for splitting some input into chunks to feed to a
// number of workers.
type Splitter interface {
	DoneReceiptChannel() chan<- interface{}
	InputReceiptChannel() chan<- []byte
	SplitResultChannel() <-chan <-chan []byte
	TotalSizeReceiptChannel() chan<- uint64
}

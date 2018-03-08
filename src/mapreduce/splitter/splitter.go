package splitter

// Splitter is responsible for splitting some input into chunks to feed to a
// number of workers.
type Splitter interface {
	DoneReceiptChannel() chan<- interface{}
	InputReceiptChannel() chan<- []byte
	TotalSizeReceiptChannel() chan<- uint64
	SeparatorToken() byte

	// This is a channel of channels because each split portions may be too large
	// to transmit all at once. Instead, we may split the delivery of each even
	// further to minimize size.
	SplitResultChannel() <-chan <-chan []byte
}

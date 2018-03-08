package splitter

import (
	"bufio"
	"bytes"
	"fmt"
)

// StringParams represents the required parameters to build a StringSplitter.
type StringParams struct {
	ChunkCount uint
	SplitToken byte
}

type stringSplitter struct {
	*StringParams
	doneReceiptCh chan interface{}
	inputCh       chan []byte
	sizeCh        chan uint64
	splitCh       chan (<-chan []byte)
}

func (ss *stringSplitter) DoneReceiptChannel() chan<- interface{} {
	return ss.doneReceiptCh
}

func (ss *stringSplitter) InputReceiptChannel() chan<- []byte {
	return ss.inputCh
}

func (ss *stringSplitter) SeparatorToken() byte {
	return ss.SplitToken
}

func (ss *stringSplitter) SplitResultChannel() <-chan <-chan []byte {
	return ss.splitCh
}

func (ss *stringSplitter) TotalSizeReceiptChannel() chan<- uint64 {
	return ss.sizeCh
}

func (ss *stringSplitter) loopWork() {
	totalSize := <-ss.sizeCh
	chunkSize := (totalSize / uint64(ss.ChunkCount)) + 1
	fmt.Println(totalSize, chunkSize)
	token := ss.SplitToken
	outCh := make(chan []byte, 0)
	ss.splitCh <- outCh
	byteOutput := 0
	totalByteCount := uint64(0)

	for {
		select {
		case input := <-ss.inputCh:
			reader := bufio.NewReader(bytes.NewBuffer(input))

			for {
				if bytes, err := reader.ReadBytes(token); err != nil && len(bytes) > 0 {
					length := len(bytes)
					byteOutput += length
					totalByteCount += uint64(length)
					outCh <- bytes

					// We need to limit each chunk to a certain size, so here we reset
					// the output channel by closing the old one and recreating a new
					// channel.
					if uint64(byteOutput) > chunkSize {
						close(outCh)
						byteOutput = 0
						outCh = make(chan []byte, 0)
						ss.splitCh <- outCh
					}
				} else {
					break
				}
			}

		case <-ss.doneReceiptCh:
			close(outCh)
			return
		}
	}
}

// NewStringSplitter returns a new StringSplitter.
func NewStringSplitter(params StringParams) Splitter {
	splitter := &stringSplitter{
		StringParams:  &params,
		doneReceiptCh: make(chan interface{}, 0),
		inputCh:       make(chan []byte, 0),
		sizeCh:        make(chan uint64, 0),
		splitCh:       make(chan (<-chan []byte), 0),
	}

	go splitter.loopWork()
	return splitter
}

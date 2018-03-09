package string

import (
	"bufio"
	"bytes"
	"fmt"

	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

// Params represents the required parameters to build a StringSplitter.
type Params struct {
	ChunkCount uint
	SplitToken byte
}

func checkParams(params *Params) *Params {
	if params.ChunkCount == 0 || params.SplitToken == 0 {
		panic("Invalid parameters")
	}

	return params
}

type stringSplitter struct {
	*Params
	doneReceiptCh chan interface{}
	inputCh       chan *util.KeyValueSize
	splitCh       chan *util.KeyValuePipe
}

func (ss *stringSplitter) DoneReceiptChannel() chan<- interface{} {
	return ss.doneReceiptCh
}

func (ss *stringSplitter) InputReceiptChannel() chan<- *util.KeyValueSize {
	return ss.inputCh
}

func (ss *stringSplitter) SeparatorToken() byte {
	return ss.SplitToken
}

func (ss *stringSplitter) SplitResultChannel() <-chan *util.KeyValuePipe {
	return ss.splitCh
}

// Since we receive key-value pipes, we need to keep a map of different keys
// and their respective output channels.
func (ss *stringSplitter) loopWork() {
	type byteOutputRequest struct {
		key      string
		outputCh chan<- int64
	}

	type chunkSizeRequest struct {
		key    string
		sizeCh chan<- int64
	}

	type outChRequest struct {
		key     string
		outChCh chan<- chan []byte
	}

	type totalSizeRequest struct {
		key    string
		sizeCh chan<- int64
	}

	type updateByteOutputRequest struct {
		key     string
		valueFn func(int64) int64
	}

	token := ss.SplitToken
	byteOutputMap := make(map[string]int64, 0)
	outChMap := make(map[string]chan []byte, 0)
	totalByteOutputMap := make(map[string]int64, 0)

	closeOutChCh := make(chan string, 0)
	getByteOutputCh := make(chan *byteOutputRequest, 0)
	getOutputChCh := make(chan *outChRequest, 0)
	getTotalByteOutputCh := make(chan *byteOutputRequest, 0)
	updateByteOutputCh := make(chan *updateByteOutputRequest, 0)
	updateTotalByteOutputCh := make(chan *updateByteOutputRequest, 0)

	go func() {
		for {
			select {
			case key := <-closeOutChCh:
				close(outChMap[key])
				delete(outChMap, key)

			case r := <-getByteOutputCh:
				r.outputCh <- byteOutputMap[r.key]

			case r := <-getOutputChCh:
				outCh, ok := outChMap[r.key]

				if !ok {
					outCh = make(chan []byte, 0)
					outChMap[r.key] = outCh

					// Send this pipe downstream immediately. The receiver will then start
					// receiving data with the output channel.
					ss.splitCh <- &util.KeyValuePipe{Key: r.key, ValueCh: outCh}
				}

				r.outChCh <- outCh

			case r := <-getTotalByteOutputCh:
				r.outputCh <- totalByteOutputMap[r.key]

			case r := <-updateByteOutputCh:
				byteOutputMap[r.key] = r.valueFn(byteOutputMap[r.key])

			case r := <-updateTotalByteOutputCh:
				totalByteOutputMap[r.key] = r.valueFn(totalByteOutputMap[r.key])
			}
		}
	}()

	for {
		select {
		case kvs := <-ss.inputCh:
			key := kvs.Key
			reader := bufio.NewReader(bytes.NewBuffer(kvs.Value))
			chunkSize := (kvs.TotalSize / int64(ss.ChunkCount)) + 1

			for {
				if bytes, err := reader.ReadBytes(token); err == nil || len(bytes) > 0 {
					length := len(bytes)

					updateByteOutputCh <- &updateByteOutputRequest{
						key: key,
						valueFn: func(current int64) int64 {
							return current + int64(length)
						},
					}

					updateTotalByteOutputCh <- &updateByteOutputRequest{
						key: key,
						valueFn: func(current int64) int64 {
							// We add back to token to accurately reflect the processed byte
							// count.
							return current + int64(length) + 1
						},
					}

					byteOutputCh := make(chan int64, 0)
					getByteOutputCh <- &byteOutputRequest{key: key, outputCh: byteOutputCh}
					byteOutput := <-byteOutputCh

					outChCh := make(chan chan []byte, 0)
					getOutputChCh <- &outChRequest{key: key, outChCh: outChCh}
					<-outChCh <- bytes

					// We need to limit each chunk to a certain size, so here we reset
					// the output channel by closing the old one and recreating a new
					// channel.
					if int64(byteOutput) > chunkSize {
						updateByteOutputCh <- &updateByteOutputRequest{
							key: key,
							valueFn: func(current int64) int64 {
								return 0
							},
						}

						closeOutChCh <- key
						outChCh = make(chan chan []byte, 0)
						getOutputChCh <- &outChRequest{key: key, outChCh: outChCh}
						outCh := <-outChCh
						ss.splitCh <- &util.KeyValuePipe{Key: key, ValueCh: outCh}
					} else {
						byteOutputCh = make(chan int64, 0)
						getTotalByteOutputCh <- &byteOutputRequest{key: key, outputCh: byteOutputCh}
						totalByteOutput := <-byteOutputCh

						fmt.Printf(
							"Total size: %d, total output: %d, remaining %d\n",
							kvs.TotalSize,
							totalByteOutput,
							kvs.TotalSize-totalByteOutput,
						)
					}
				} else {
					break
				}
			}
		}
	}
}

// NewStringSplitter returns a new StringSplitter.
func NewStringSplitter(params Params) sp.Splitter {
	checked := checkParams(&params)

	splitter := &stringSplitter{
		Params:        checked,
		doneReceiptCh: make(chan interface{}, 0),
		inputCh:       make(chan *util.KeyValueSize, 0),
		splitCh:       make(chan *util.KeyValuePipe, 0),
	}

	go splitter.loopWork()
	return splitter
}

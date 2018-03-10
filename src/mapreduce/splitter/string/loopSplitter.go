package string

import (
	"bufio"
	"bytes"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

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
		key      string
		outputCh chan<- int64
		valueFn  func(int64) int64
	}

	token := ss.SplitToken
	byteOutputMap := make(map[string]int64, 0)
	outChMap := make(map[string]chan []byte, 0)
	totalByteOutputMap := make(map[string]int64, 0)

	closeOutChCh := make(chan string, 0)
	getOutputChCh := make(chan *outChRequest, 0)
	updateByteOutputCh := make(chan *updateByteOutputRequest, 0)
	updateTotalByteOutputCh := make(chan *updateByteOutputRequest, 0)

	go func() {
		for {
			select {
			case key := <-closeOutChCh:
				close(outChMap[key])

				// We delete the previous output channel from the map. The next time
				// it is accessed, we create a new channel and deposit it downstream.
				delete(outChMap, key)

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

			case r := <-updateByteOutputCh:
				byteOutputMap[r.key] = r.valueFn(byteOutputMap[r.key])

				// We allow the output channel to be nil if we are not interested in
				// the result.
				if r.outputCh != nil {
					r.outputCh <- byteOutputMap[r.key]
				}

			case r := <-updateTotalByteOutputCh:
				totalByteOutputMap[r.key] = r.valueFn(totalByteOutputMap[r.key])
				r.outputCh <- totalByteOutputMap[r.key]
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
				if bytes, err := reader.ReadBytes(token); err == nil ||
					(bytes != nil && len(bytes) > 0) {
					length := len(bytes)

					// We add back the separator token to accurately reflect the processed
					// byte count.
					lengthIncToken := length + 1

					byteOutputCh := make(chan int64, 0)

					updateByteOutputCh <- &updateByteOutputRequest{
						key:      key,
						outputCh: byteOutputCh,
						valueFn: func(current int64) int64 {
							return current + int64(lengthIncToken)
						},
					}

					byteOutput := <-byteOutputCh

					totalByteOutputCh := make(chan int64, 0)

					updateTotalByteOutputCh <- &updateByteOutputRequest{
						key:      key,
						outputCh: totalByteOutputCh,
						valueFn: func(current int64) int64 {
							return current + int64(lengthIncToken)
						},
					}

					totalByteOutput := <-totalByteOutputCh

					outChCh := make(chan chan []byte, 0)
					getOutputChCh <- &outChRequest{key: key, outChCh: outChCh}
					<-outChCh <- bytes

					// We need to limit each chunk to a certain size, so here we reset
					// the output channel by closing the old one and recreating a new
					// channel.
					if int64(byteOutput) > chunkSize {
						updateByteOutputCh <- &updateByteOutputRequest{
							key:      key,
							outputCh: nil,
							valueFn: func(current int64) int64 {
								return 0
							},
						}

						closeOutChCh <- key

						// Beware that the new output channel is deposited downstream here,
						// so we do not need to explicitly do so.
						outChCh = make(chan chan []byte, 0)
						getOutputChCh <- &outChRequest{key: key, outChCh: outChCh}
						<-outChCh
					}

					// If the total byte output is equal to/larger than the total size,
					// we must have finished receiving all contents of the input. The
					// reason why it is not exactly equal is due to out addition of the
					// token byte length to the byte output calculation. If the separator
					// token is a newline character, the last line of the document would
					// not need it to be added back, so we end up with one extra token
					// charactor.
					if totalByteOutput >= kvs.TotalSize {
						ss.LogMan.Printf("Finished splitting input for %s\n", key)
						closeOutChCh <- key
					}
				} else {
					break
				}
			}
		}
	}
}

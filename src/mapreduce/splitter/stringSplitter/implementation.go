package stringSplitter

import (
	"bufio"
	"bytes"
)

func (ss *stringSplitter) String() string {
	return "String splitter"
}

func (ss *stringSplitter) SplitInput(inputCh <-chan []byte, totalSize int64) <-chan [][]byte {
	outCh := make(chan [][]byte)

	go func() {
		byteOutput := int64(0)
		chunkSize := (totalSize / int64(ss.ChunkCount)) + 1
		intermediate := make([][]byte, 0)
		token := ss.SplitToken

		for {
			select {
			case input, ok := <-inputCh:
				if !ok {
					// Send whatever that has not been sent upstream.
					if len(intermediate) > 0 {
						outCh <- intermediate
					}

					close(outCh)
					ss.LogMan.Printf("%v: finished splitting input\n", ss)
					return
				}

				reader := bufio.NewReader(bytes.NewBuffer(input))

				for {
					if bytes, err := reader.ReadBytes(token); err == nil || len(bytes) > 0 {
						// Add back the separator token.
						byteOutput += int64(len(bytes)) + 1
						intermediate = append(intermediate, bytes)

						// We need to limit each chunk to a certain size, so here we reset
						// the output channel by closing the old one and recreating a new
						// channel.
						if byteOutput > chunkSize {
							byteOutput = 0
							outCh <- intermediate
							intermediate = make([][]byte, 0)
						}
					} else {
						break
					}
				}
			}
		}
	}()

	return outCh
}

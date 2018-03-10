package local

import (
	"bufio"
	"os"
	"strings"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"

	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
)

func (lr *localInputReader) readFileAtPath(path string) {
	file, err := os.Open(path)

	if err != nil {
		lr.errCh <- &ir.Error{Original: err}
		return
	}

	defer file.Close()
	fInfo, err := file.Stat()

	if err != nil {
		lr.errCh <- &ir.Error{Original: err}
		return
	}

	size := fInfo.Size()
	valueCh := make(chan []byte, 0)
	go lr.splitInput(valueCh, path, size)

	// Now we scan the file contents.
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		valueCh <- scanner.Bytes()
	}

	close(valueCh)
}

// Split input and transmit downstream. Before transmission, we need to join
// the split result bytes with newlines to compensate for the scanning process.
func (lr *localInputReader) splitInput(inputCh <-chan []byte, key string, totalSize int64) {
	joinBytes := func(bytes [][]byte) []byte {
		strs := make([]string, 0)

		for ix := range bytes {
			strs = append(strs, string(bytes[ix]))
		}

		return []byte(strings.Join(strs, "\n"))
	}

	nChunk := 0
	resetCh := make(chan interface{}, 1)
	splitResultCh := lr.Splitter.SplitInput(inputCh, totalSize)
	splitCh := splitResultCh
	var output *util.KeyValueChunk
	var readOutputCh chan *util.KeyValueChunk

	for {
		select {
		case chunk, ok := <-splitCh:
			if !ok {
				return
			}

			splitCh = nil
			nChunk++
			readOutputCh = lr.readCh
			joined := joinBytes(chunk)
			output = &util.KeyValueChunk{Key: key, Value: joined, NChunk: nChunk}

		case readOutputCh <- output:
			readOutputCh = nil
			output = nil
			resetCh <- true

		case <-resetCh:
			splitCh = splitResultCh
		}
	}
}

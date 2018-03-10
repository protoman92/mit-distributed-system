package local

import (
	"bufio"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"

	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
)

func (lr *localInputReader) loopWork(path string) {
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
	valuePipeCh := make(chan []byte, 0)

	kvPipe := &util.KeyValuePipe{
		Key:       path,
		TotalSize: size,
		ValueCh:   valuePipeCh,
	}

	lr.readCh <- kvPipe

	// Now we scan the file contents and transmit them downstream via the value
	// pipe.
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		valuePipeCh <- scanner.Bytes()
	}

	close(valuePipeCh)
}

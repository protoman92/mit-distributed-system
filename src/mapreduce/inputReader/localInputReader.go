package inputReader

import (
	"bufio"
	"os"
	"path"
)

// LocalParams represents the required parameters to build a LocalInputReader.
type LocalParams struct {
	FileName string
	FileDir  string
}

type localInputReader struct {
	*LocalParams
	doneInputCh chan interface{}
	errCh       chan error
	readCh      chan []byte
	sizeCh      chan uint64
}

func (lr *localInputReader) DoneInputChannel() <-chan interface{} {
	return lr.doneInputCh
}

func (lr *localInputReader) ErrorChannel() <-chan error {
	return lr.errCh
}

func (lr *localInputReader) ReadInputChannel() <-chan []byte {
	return lr.readCh
}

func (lr *localInputReader) TotalSizeChannel() <-chan uint64 {
	return lr.sizeCh
}

func (lr *localInputReader) loopWork() {
	fullPath := path.Join(lr.FileDir, lr.FileName)
	file, err := os.Open(fullPath)

	if err != nil {
		lr.errCh <- &Error{Original: err}
		return
	}

	defer file.Close()
	fInfo, err := file.Stat()

	if err != nil {
		lr.errCh <- &Error{Original: err}
		return
	}

	size := fInfo.Size()
	lr.sizeCh <- uint64(size)
	inputReadCh := lr.readCh
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		inputReadCh <- scanner.Bytes()
	}

	lr.doneInputCh <- true
}

// NewLocalInputReader returns a new LocalInputReader.
func NewLocalInputReader(params LocalParams) InputReader {
	lr := &localInputReader{
		LocalParams: &params,
		doneInputCh: make(chan interface{}, 0),
		errCh:       make(chan error, 1),
		readCh:      make(chan []byte, 0),
		sizeCh:      make(chan uint64, 1),
	}

	go lr.loopWork()
	return lr
}

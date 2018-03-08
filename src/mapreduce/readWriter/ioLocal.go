package readWriter

import (
	"bufio"
	"os"
	"path"
)

// LocalParams represents the required parameters to build a LocalReadWriter.
type LocalParams struct {
	FileName string
	FileDir  string
}

type localReadWriter struct {
	*LocalParams
	doneInputCh chan interface{}
	errCh       chan error
	readCh      chan []byte
	sizeCh      chan uint64
}

func (lrw *localReadWriter) DoneInputChannel() <-chan interface{} {
	return lrw.doneInputCh
}

func (lrw *localReadWriter) ErrorChannel() <-chan error {
	return lrw.errCh
}

func (lrw *localReadWriter) ReadInputChannel() <-chan []byte {
	return lrw.readCh
}

func (lrw *localReadWriter) TotalSizeChannel() <-chan uint64 {
	return lrw.sizeCh
}

func (lrw *localReadWriter) loopWork() {
	fullPath := path.Join(lrw.FileDir, lrw.FileName)
	file, err := os.Open(fullPath)

	if err != nil {
		lrw.errCh <- err
		return
	}

	defer file.Close()
	fInfo, err := file.Stat()

	if err != nil {
		lrw.errCh <- err
		return
	}

	size := fInfo.Size()
	lrw.sizeCh <- uint64(size)
	inputReadCh := lrw.readCh
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		inputReadCh <- scanner.Bytes()
	}

	lrw.doneInputCh <- true
}

// NewLocalReadWriter returns a new LocalReadWriter.
func NewLocalReadWriter(params LocalParams) ReadWriter {
	lrw := &localReadWriter{
		LocalParams: &params,
		doneInputCh: make(chan interface{}, 0),
		errCh:       make(chan error, 1),
		readCh:      make(chan []byte, 0),
		sizeCh:      make(chan uint64, 1),
	}

	go lrw.loopWork()
	return lrw
}

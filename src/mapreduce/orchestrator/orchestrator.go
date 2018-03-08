package orchestrator

import (
	exc "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/executor"
	rw "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/readWriter"
	sp "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/splitter"
)

// Orchestrator represents a MapReduce process orchestrator. When initialized,
// it will start processing input right away, and then delegate the processing
// to its executor (which in turn may hand over portions of the work to workers
// in a distributed system).
type Orchestrator interface {
	ErrorChannel() <-chan error
	DoneChannel() <-chan interface{}
}

// Params represents the required parameters to build an Orchestrator.
type Params struct {
	Executor   exc.Executor
	ReadWriter rw.ReadWriter
	Splitter   sp.Splitter
}

type orchestrator struct {
	*Params
	errCh chan error
}

func (o *orchestrator) DoneChannel() <-chan interface{} {
	return o.Executor.DoneChannel()
}

func (o *orchestrator) ErrorChannel() <-chan error {
	return o.errCh
}

func (o *orchestrator) loopReadInput() {
	resetCh := make(chan interface{}, 1)
	rwInputCh := o.ReadWriter.ReadInputChannel()
	var input []byte
	var splitterInputCh chan<- []byte

	for {
		select {
		case input = <-rwInputCh:
			rwInputCh = nil
			splitterInputCh = o.Splitter.InputReceiptChannel()

		case splitterInputCh <- input:
			splitterInputCh = nil
			input = nil
			resetCh <- true

		case <-resetCh:
			rwInputCh = o.ReadWriter.ReadInputChannel()
		}
	}
}

// We need to have a separate loop to transmit the size because this value will
// not be known until the input is read (or a header is received in a network
// response).
func (o *orchestrator) loopTransmitInputSize() {
	var receiveCh chan<- uint64
	var size uint64

	for {
		select {
		case size = <-o.ReadWriter.TotalSizeChannel():
			receiveCh = o.Splitter.TotalSizeReceiptChannel()

		case receiveCh <- size:
			return
		}
	}
}

func (o *orchestrator) loopReceiveSplitResult() {
	splitResultCh := o.Splitter.SplitResultChannel()
	resetCh := make(chan interface{}, 1)
	splitToken := o.Splitter.SeparatorToken()
	var data []byte
	var excInputCh chan<- []byte

	for {
		select {
		case splitCh := <-splitResultCh:
			splitResultCh = nil
			data = make([]byte, 0)

			// We must be sure to close the split channels.
			for split := range splitCh {
				data = append(data, split...)

				// We need to add back the separator.
				data = append(data, splitToken)
			}

			excInputCh = o.Executor.InputReceiptChannel()

		case excInputCh <- data:
			excInputCh = nil
			data = nil
			resetCh <- true

		case <-resetCh:
			splitResultCh = o.Splitter.SplitResultChannel()
		}
	}
}

func (o *orchestrator) loopDoneInput() {
	rwDoneInputCh := o.ReadWriter.DoneInputChannel()
	resetCh := make(chan interface{}, 1)
	var splitDoneReceiptCh chan<- interface{}

	for {
		select {
		case <-rwDoneInputCh:
			rwDoneInputCh = nil
			splitDoneReceiptCh = o.Splitter.DoneReceiptChannel()

		case splitDoneReceiptCh <- true:
			splitDoneReceiptCh = nil
			resetCh <- true

		case <-resetCh:
			rwDoneInputCh = o.ReadWriter.DoneInputChannel()
		}
	}
}

func (o *orchestrator) loopError() {
	for {
		select {
		case err := <-o.ReadWriter.ErrorChannel():
			o.errCh <- err
		}
	}
}

// NewOrchestrator returns a new Orchestrator.
func NewOrchestrator(params *Params) Orchestrator {
	orchestrator := &orchestrator{Params: params, errCh: make(chan error, 0)}
	go orchestrator.loopDoneInput()
	go orchestrator.loopError()
	go orchestrator.loopReadInput()
	go orchestrator.loopReceiveSplitResult()
	go orchestrator.loopTransmitInputSize()
	return orchestrator
}

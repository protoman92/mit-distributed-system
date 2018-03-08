package orchestrator

import (
	"fmt"

	exc "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/executor"
	rw "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/readWriter"
	sp "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/splitter"
)

// Orchestrator represents a MapReduce process orchestrator.
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

func (o *orchestrator) loopInputTransmission() {
	executor := o.Executor
	readWriter := o.ReadWriter
	splitter := o.Splitter
	resetCh := make(chan interface{}, 1)
	rwInputCh := readWriter.ReadInputChannel()
	var execInputCh chan<- []byte
	var execPendingSplitCh <-chan []byte
	var input []byte
	var splitterInputCh chan<- []byte

	for {
		select {
		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		case input = <-rwInputCh:
			rwInputCh = nil
			execInputCh = executor.InputReceiptChannel()

		case execInputCh <- input:
			execInputCh = nil
			input = nil
			execPendingSplitCh = executor.InputPendingSplitChannel()

		case input = <-execPendingSplitCh:
			execPendingSplitCh = nil
			splitterInputCh = splitter.InputReceiptChannel()

		case splitterInputCh <- input:
			splitterInputCh = nil
			input = nil
			resetCh <- true
		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

		case <-resetCh:
			rwInputCh = readWriter.ReadInputChannel()
		}
	}
}

func (o *orchestrator) loopSize() {
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

func (o *orchestrator) loopSplitResult() {
	for {
		select {
		case splitCh := <-o.Splitter.SplitResultChannel():
			for data := range splitCh {
				fmt.Println(string(data))
			}
		}
	}
}

func (o *orchestrator) loopDoneInput() {
	rwDoneInputCh := o.ReadWriter.DoneInputChannel()
	resetCh := make(chan interface{}, 1)
	var executorDoneInputCh chan<- interface{}
	var executorDonePendingSplitCh <-chan interface{}
	var splitDoneReceiptCh chan<- interface{}

	for {
		select {
		case <-rwDoneInputCh:
			rwDoneInputCh = nil
			executorDoneInputCh = o.Executor.DoneInputChannel()

		case executorDoneInputCh <- true:
			executorDoneInputCh = nil
			executorDonePendingSplitCh = o.Executor.DonePendingSplitChannel()

		case <-executorDonePendingSplitCh:
			executorDonePendingSplitCh = nil
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
	go orchestrator.loopInputTransmission()
	go orchestrator.loopSize()
	go orchestrator.loopSplitResult()
	return orchestrator
}

package orchestrator

import (
	exc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
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
	Executor    exc.Executor
	InputReader ir.InputReader
	Splitter    sp.Splitter
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

// NewOrchestrator returns a new Orchestrator.
func NewOrchestrator(params *Params) Orchestrator {
	orchestrator := &orchestrator{Params: params, errCh: make(chan error, 0)}
	go orchestrator.loopDoneInput()
	go orchestrator.loopError()
	go orchestrator.loopReadInput()
	go orchestrator.loopReceiveSplitResult()
	return orchestrator
}

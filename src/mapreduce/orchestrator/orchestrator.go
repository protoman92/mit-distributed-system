package orchestrator

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
	"github.com/protoman92/mit-distributed-system/src/util"
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
	Executor    executor.Executor
	InputReader inputReader.InputReader
	LogMan      util.LogMan
}

func checkParams(params *Params) *Params {
	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{})
	}

	return params
}

type orchestrator struct {
	*Params
	errCh chan error
}

// NewOrchestrator returns a new Orchestrator.
func NewOrchestrator(params *Params) Orchestrator {
	checked := checkParams(params)
	orchestrator := &orchestrator{Params: checked, errCh: make(chan error, 0)}
	go orchestrator.loopError()
	go orchestrator.loopReadInput()
	return orchestrator
}

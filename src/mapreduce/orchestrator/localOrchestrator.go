package orchestrator

import (
	erpc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor/rpc"
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader"
	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
)

// LocalParams represents the requires parameters to build a local orchestrator.
type LocalParams struct {
	ExecutorParams    erpc.Params
	InputReaderParams ir.LocalParams
	SplitterParams    sp.StringParams
}

// NewLocalOrchestrator returns a new local Orchestrator.
func NewLocalOrchestrator(params LocalParams) Orchestrator {
	oParams := &Params{
		Executor:    erpc.NewRPCMasterExecutor(params.ExecutorParams),
		InputReader: ir.NewLocalInputReader(params.InputReaderParams),
		Splitter:    sp.NewStringSplitter(params.SplitterParams),
	}

	return NewOrchestrator(oParams)
}

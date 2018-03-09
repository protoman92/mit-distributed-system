package local

import (
	erpc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor/rpc"
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader/local"
	orc "github.com/protoman92/mit-distributed-system/src/mapreduce/orchestrator"
	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter/string"
)

// Params represents the requires parameters to build a local orchestrator.
type Params struct {
	ExecutorParams    erpc.Params
	InputReaderParams ir.Params
	SplitterParams    sp.Params
}

// NewLocalOrchestrator returns a new local Orchestrator.
func NewLocalOrchestrator(params Params) orc.Orchestrator {
	oParams := &orc.Params{
		Executor:    erpc.NewRPCMasterExecutor(params.ExecutorParams),
		InputReader: ir.NewLocalInputReader(params.InputReaderParams),
		Splitter:    sp.NewStringSplitter(params.SplitterParams),
	}

	return orc.NewOrchestrator(oParams)
}

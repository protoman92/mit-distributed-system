package localOrc

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader/localReader"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/orchestrator"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Params represents the requires parameters to build a local orchestrator.
type Params struct {
	ExecutorParams    executor.Params
	InputReaderParams localReader.Params
	LogMan            util.LogMan
}

// NewLocalOrchestrator returns a new local Orchestrator.
func NewLocalOrchestrator(params Params) orchestrator.Orchestrator {
	oParams := &orchestrator.Params{
		Executor:    executor.NewRPCMasterExecutor(params.ExecutorParams),
		InputReader: localReader.NewLocalInputReader(params.InputReaderParams),
		LogMan:      params.LogMan,
	}

	return orchestrator.NewOrchestrator(oParams)
}

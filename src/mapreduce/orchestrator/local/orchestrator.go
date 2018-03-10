package local

import (
	exc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader/local"
	orc "github.com/protoman92/mit-distributed-system/src/mapreduce/orchestrator"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Params represents the requires parameters to build a local orchestrator.
type Params struct {
	ExecutorParams    exc.Params
	InputReaderParams ir.Params
	LogMan            util.LogMan
}

// NewLocalOrchestrator returns a new local Orchestrator.
func NewLocalOrchestrator(params Params) orc.Orchestrator {
	oParams := &orc.Params{
		Executor:    exc.NewRPCMasterExecutor(params.ExecutorParams),
		InputReader: ir.NewLocalInputReader(params.InputReaderParams),
		LogMan:      params.LogMan,
	}

	return orc.NewOrchestrator(oParams)
}

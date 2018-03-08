package orchestrator

import (
	exc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	rw "github.com/protoman92/mit-distributed-system/src/mapreduce/readWriter"
	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
)

// LocalParams represents the requires parameters to build a local orchestrator.
type LocalParams struct {
	ExecutorParams   exc.Params
	ReadWriterParams rw.LocalParams
	SplitterParams   sp.StringParams
}

// NewLocalOrchestrator returns a new local Orchestrator.
func NewLocalOrchestrator(params LocalParams) Orchestrator {
	oParams := &Params{
		Executor:   exc.NewExecutor(params.ExecutorParams),
		ReadWriter: rw.NewLocalReadWriter(params.ReadWriterParams),
		Splitter:   sp.NewStringSplitter(params.SplitterParams),
	}

	return NewOrchestrator(oParams)
}

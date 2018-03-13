package worker

import (
	"github.com/protoman92/mit-distributed-system/src/util"
)

func checkParams(params *Params) *Params {
	if params.Delegate == nil ||
		params.JobCapacity == 0 ||
		params.Mapper == nil ||
		params.MasterAddress == "" ||
		params.MasterCompleteJobMethod == "" ||
		params.MasterRegisterMethod == "" ||
		params.Reducer == nil ||
		params.RPCHandler == nil {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkWorker(worker *worker) {
	if worker.capacityCh == nil ||
		worker.jobQueueCh == nil ||
		worker.shutdownCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(delegate *WkDelegate) {
	if delegate.accessFileCh == nil || delegate.jobCh == nil {
		panic("Invalid setup")
	}
}

package worker

import "github.com/protoman92/mit-distributed-system/src/util"

func checkParams(params *Params) *Params {
	if params.MasterRegisterMethod == "" {
		panic("Invalid parameters")
	}

	if params.LogMan != nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkWorker(worker *worker) {
	if worker.errCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(delegate *WkDelegate) {}

package master

import "github.com/protoman92/mit-distributed-system/src/util"

func checkParams(params *Params) *Params {
	if params.ExpectedWorkerCount == 0 ||
		params.PingPeriod == 0 ||
		params.RetryDuration == 0 ||
		params.State == nil ||
		params.WorkerAcceptJobMethod == "" ||
		params.WorkerPingMethod == "" {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkMaster(master *master) {
	if master.completionCh == nil ||
		master.errCh == nil ||
		master.delegate == nil ||
		master.shutdownCh == nil ||
		master.workers == nil ||
		master.workerQueueCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(d *MstDelegate) {
	if d.jobCompleteCh == nil ||
		d.jobRequestCh == nil ||
		d.registerWorkerCh == nil {
		panic("Invalid delegate")
	}
}

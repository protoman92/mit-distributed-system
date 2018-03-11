package master

import "github.com/protoman92/mit-distributed-system/src/util"

func checkParams(params *Params) *Params {
	if params.PingPeriod == 0 {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkMaster(master *master) {
	if master.errCh == nil ||
		master.delegate == nil ||
		master.shutdownCh == nil ||
		master.workers == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(d *MstDelegate) {
	if d.jobRequestCh == nil || d.registerWorkerCh == nil {
		panic("Invalid delegate")
	}
}

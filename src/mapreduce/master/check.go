package master

import "github.com/protoman92/mit-distributed-system/src/util"

func checkParams(params *Params) *Params {
	if params.Address == "" || params.Network == "" {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkMaster(master *master) {
	if master.errCh == nil ||
		master.jobRequestCh == nil ||
		master.rpcShutdownCh == nil ||
		master.shutdownCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(d *MstDelegate) {
	if d.jobRequestCh == nil || d.shutdownCh == nil {
		panic("Invalid delegate")
	}
}

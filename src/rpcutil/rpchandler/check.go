package rpchandler

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

func checkHandler(handler *handler) {
	if handler.errCh == nil ||
		handler.delegate == nil ||
		handler.shutdownCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(delegate *RPCDelegate) {
	if delegate.shutdownCh == nil {
		panic("Invalid setup")
	}
}

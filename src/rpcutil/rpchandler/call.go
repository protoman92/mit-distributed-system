package rpchandler

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

func (h *handler) Call(params rpcutil.CallParams) error {
	return h.Caller.Call(params)
}

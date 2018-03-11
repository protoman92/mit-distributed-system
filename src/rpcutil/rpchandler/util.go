package rpchandler

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// Shutdown sends a shutdown request to a target using a common shutdown RPC
// method.
func Shutdown(network, target string) error {
	request := &ShutdownRequest{}
	reply := &ShutdownReply{}

	callParams := rpcutil.CallParams{
		Args:    request,
		Method:  "RPCDelegate.Shutdown",
		Network: network,
		Reply:   reply,
		Target:  target,
	}

	return rpcutil.Call(callParams)
}

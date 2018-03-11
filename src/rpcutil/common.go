package rpcutil

import (
	"net/rpc"
)

// CallParams represents the required parameters to perform an RPC call.
type CallParams struct {
	Args, Reply             interface{}
	Method, Network, Target string
}

// Call calls a RPC method for a target addressn.
func Call(params CallParams) error {
	conn, err := rpc.Dial(params.Network, params.Target)

	if err != nil {
		return err
	}

	defer conn.Close()
	return conn.Call(params.Method, params.Args, params.Reply)
}

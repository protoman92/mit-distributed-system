package rpc

import "github.com/protoman92/mit-distributed-system/src/mapreduce/rpcutil"

// Notify the master that this worker is ready to accept work. If an error
// occurs, attempt registration again.
func (w *servant) registerWithMaster() {
	registerParams := &RegisterParams{WorkerAddress: w.Params.Address}
	registerReply := &RegisterReply{}

	params := &rpcutil.CallParams{
		Args:    registerParams,
		Method:  w.Params.MasterRegisterMethod,
		Network: w.Params.Network,
		Reply:   registerReply,
		Target:  w.Params.MasterAddress,
	}

	if err := rpcutil.Call(params); err != nil {
		w.registerWithMaster()
	}
}

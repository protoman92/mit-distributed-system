package worker

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

func (w *worker) registerWithMaster() {
	request := &RegisterRequest{WorkerAddress: w.RPCParams.Address}
	reply := &ResigterReply{}

	callParams := rpcutil.CallParams{
		Args:    request,
		Method:  w.MasterRegisterMethod,
		Network: w.RPCParams.Network,
		Reply:   reply,
		Target:  w.MasterAddress,
	}

	if err := rpcutil.Call(callParams); err != nil {
		go func() {
			w.errCh <- err
		}()
	}
}

package worker

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// Register with master every time a capacity slot is released.
func (w *worker) loopRegister() {
	for {
		select {
		case <-w.shutdownCh:
			return

		default:
			// Take a look at the code for job request loop for the other side of this
			// channel.
			w.capacityCh <- true
			w.RPCParams.RetryWithDelay(w.registerWithMaster)()
		}
	}
}

func (w *worker) registerWithMaster() error {
	request := &RegisterRequest{WorkerAddress: w.RPCParams.Address}
	reply := &ResigterReply{}

	callParams := rpcutil.CallParams{
		Args:    request,
		Method:  w.MasterRegisterMethod,
		Network: w.RPCParams.Network,
		Reply:   reply,
		Target:  w.MasterAddress,
	}

	return w.RPCHandler.Call(callParams)
}

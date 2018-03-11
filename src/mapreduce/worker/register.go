package worker

import (
	"github.com/protoman92/gocompose/pkg"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// Register with master every time a capacity slot is released.
func (w *worker) loopRegister() {
	for {
		// Take a look at the code for job request loop for the other side of this
		// channel.
		w.capacityCh <- true
		w.registerWithMaster()
	}
}

func (w *worker) registerWithMaster() {
	register := func() error {
		request := &RegisterRequest{WorkerAddress: w.RPCParams.Address}
		reply := &ResigterReply{}

		callParams := rpcutil.CallParams{
			Args:    request,
			Method:  w.MasterRegisterMethod,
			Network: w.RPCParams.Network,
			Reply:   reply,
			Target:  w.MasterAddress,
		}

		return rpcutil.Call(callParams)
	}

	if err := compose.Retry(register, w.RPCParams.RetryCount)(); err != nil {
		w.errCh <- err
	}
}

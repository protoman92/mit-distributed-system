package master

import (
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

func (m *master) loopPing(w string) {
	for {
		select {
		case <-m.shutdownCh:
			return

		case <-time.After(m.PingPeriod):
			m.ping(w)
		}
	}
}

func (m *master) ping(w string) {
	// m.LogMan.Printf("%v: pinging worker %s\n", m, w)
	args := &worker.PingRequest{}
	reply := &worker.PingReply{}

	callParams := rpcutil.CallParams{
		Args:    args,
		Method:  m.WorkerPingMethod,
		Network: m.RPCParams.Network,
		Reply:   reply,
		Target:  w,
	}

	if err := m.RPCHandler.Call(callParams); err != nil || !reply.OK {
		m.LogMan.Printf("%v: worker %s timed out (error %v)\n", m, w, err)
	}
}

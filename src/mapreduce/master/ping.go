package master

import (
	"time"

	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

func (m *master) loopPing(w string) {
	for {
		select {
		case <-m.shutdownCh:
			m.LogMan.Printf("%v: shutting down ping for worker %s.\n", m, w)
			return

		case <-time.After(m.PingPeriod):
			m.ping(w)
		}
	}
}

func (m *master) ping(w string) {
	m.LogMan.Printf("%v: pinging worker %s\n", m, w)
	args := &worker.PingRequest{}
	reply := &worker.PingReply{}

	callParams := rpcutil.CallParams{
		Args:    args,
		Method:  "WkDelegate.Ping",
		Network: m.RPCParams.Network,
		Reply:   reply,
		Target:  w,
	}

	if err := rpcutil.Call(callParams); err != nil || !reply.OK {
		m.LogMan.Printf("%v: worker %s timed out (error %v)\n", m, w, err)
	} else {
		m.LogMan.Printf("%v: received response from worker %s\n", m, w)
	}
}

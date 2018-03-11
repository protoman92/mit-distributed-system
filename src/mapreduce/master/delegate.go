package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// MstDelegate represents a Master's RPC delegate that can be exported and
// relays requests to the master.
type MstDelegate struct {
	jobRequestCh     chan *JobCallResult
	registerWorkerCh chan *worker.RegisterCallResult
}

func newDelegate() *MstDelegate {
	return &MstDelegate{
		jobRequestCh:     make(chan *JobCallResult, 0),
		registerWorkerCh: make(chan *worker.RegisterCallResult, 0),
	}
}

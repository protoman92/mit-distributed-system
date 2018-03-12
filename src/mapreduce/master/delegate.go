package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// MstDelegate represents a Master's RPC delegate that can be exported and
// relays requests to the master.
type MstDelegate struct {
	jobCompleteCh    chan worker.JobCallResult
	jobRequestCh     chan JobCallResult
	registerWorkerCh chan worker.RegisterCallResult
}

func newDelegate() *MstDelegate {
	return &MstDelegate{
		jobCompleteCh:    make(chan worker.JobCallResult, 0),
		jobRequestCh:     make(chan JobCallResult, 0),
		registerWorkerCh: make(chan worker.RegisterCallResult, 0),
	}
}

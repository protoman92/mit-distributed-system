package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// MstDelegate represents a Master's RPC delegate that can be exported and
// relays requests to the master.
type MstDelegate struct {
	jobCompleteCh    chan worker.JobCallResult
	registerWorkerCh chan worker.RegisterCallResult
}

// NewDelegate returns a new MstDelegate.
func NewDelegate() *MstDelegate {
	delegate := &MstDelegate{
		jobCompleteCh:    make(chan worker.JobCallResult, 0),
		registerWorkerCh: make(chan worker.RegisterCallResult, 0),
	}

	checkDelegate(delegate)
	return delegate
}

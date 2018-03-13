package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccess"
)

// WkDelegate represents a RPC Delegate for a worker.
type WkDelegate struct {
	accessFileCh chan fileaccess.AccessCallResult
	jobCh        chan JobCallResult
}

// NewDelegate returns a new WkDelegate.
func NewDelegate() *WkDelegate {
	delegate := &WkDelegate{
		accessFileCh: make(chan fileaccess.AccessCallResult, 0),
		jobCh:        make(chan JobCallResult, 0),
	}

	checkDelegate(delegate)
	return delegate
}

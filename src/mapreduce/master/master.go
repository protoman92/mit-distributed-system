package master

import (
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
)

// Master represents a master that receives job requests from some client and
// distributes them to workers.
type Master interface {
	// This is just for convenience. A better implementation would be a RPC to
	// the client notifying it of the completion of a job request.
	AllCompletedChannel() <-chan interface{}
	ShutdownChannel() <-chan interface{}
}

type master struct {
	*Params
	mutex          *sync.RWMutex
	workers        []string
	allCompletedCh chan interface{}
	jobCompleteCh  chan job.WorkerJob
	shutdownCh     chan interface{}
	workerQueueCh  chan string
}

// NewMaster returns a new Master.
func NewMaster(params Params) Master {
	checked := checkParams(&params)
	delegate := NewDelegate()
	checkDelegate(delegate)

	master := &master{
		Params:         checked,
		mutex:          &sync.RWMutex{},
		workers:        make([]string, 0),
		allCompletedCh: make(chan interface{}, 0),
		jobCompleteCh:  make(chan job.WorkerJob, 0),
		shutdownCh:     make(chan interface{}, 0),
		workerQueueCh:  make(chan string, params.ExpectedWorkerCount),
	}

	checkMaster(master)
	go master.loopAllCompletion()
	go master.loopJobAssignment()
	go master.loopJobComplete()
	go master.loopJobCompleteNotification()
	go master.loopRegister()
	go master.loopShutdown()
	go master.updateWorkerJobs()
	return master
}

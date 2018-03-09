package rpc

import (
	"fmt"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

// RegisterParams represents the required parameters to register a worker.
type RegisterParams struct {
	WorkerAddress string
}

// RegisterReply represents the reply to a registration request.
type RegisterReply struct{}

// JobParams represents the required parameters to perform a job.
type JobParams struct {
	Data      []byte
	Key       string
	JobNumber uint
	JobType   util.JobType
}

func (p *JobParams) String() string {
	return fmt.Sprintf(
		"Key %s, job number %d, type %s, data count %d",
		p.Key,
		p.JobNumber,
		p.JobType,
		len(p.Data),
	)
}

// JobReply represents the reply to a job request.
type JobReply struct{}

// ShutdownParams represents the required parameters to perform a shutdown.
type ShutdownParams struct{}

// ShutdownReply represents the response from a shutdown.
type ShutdownReply struct{}

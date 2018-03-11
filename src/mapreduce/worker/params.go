package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Params represents the required parameters to build a Worker.
type Params struct {
	RPCParams            rpchandler.Params
	LogMan               util.LogMan
	MasterAddress        string
	MasterRegisterMethod string
}

// JobRequest represents a Map/Reduce job request.
type JobRequest struct {
	FilePath string
	Type     mrutil.TaskType
}

// JobReply represents a reply to a job request.
type JobReply struct{}

// JobCallResult represents the result of a job request invocation.
type JobCallResult struct {
	request *JobRequest
	errCh   chan error
}

// PingRequest represents a ping request to notify the master of activity.
type PingRequest struct{}

// PingReply represents the result of a ping request.
type PingReply struct {
	OK bool
}

// RegisterRequest represents a register request from a worker.
type RegisterRequest struct {
	WorkerAddress string
}

// ResigterReply represents a reply to a registration request.
type ResigterReply struct{}

// RegisterCallResult represents the result of a request invocation.
type RegisterCallResult struct {
	Request *RegisterRequest
	ErrCh   chan error
}

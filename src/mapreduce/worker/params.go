package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/reducer"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Params represents the required parameters to build a Worker.
type Params struct {
	RPCParams               rpchandler.Params
	Delegate                *WkDelegate
	LogMan                  util.LogMan
	JobCapacity             uint
	Mapper                  mapper.Mapper
	MasterAddress           string
	MasterCompleteJobMethod string
	MasterRegisterMethod    string
	Reducer                 reducer.Reducer
	RPCHandler              rpchandler.Handler
}

// JobReply represents a reply to a job request.
type JobReply struct{}

// JobCallResult represents the result of a job request invocation.
type JobCallResult struct {
	Request job.WorkerJob
	ErrCh   chan error
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
	Request RegisterRequest
	ErrCh   chan error
}

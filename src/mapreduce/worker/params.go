package worker

import (
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
type JobRequest struct{}

// JobReply represents a reply to a job request.
type JobReply struct{}

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

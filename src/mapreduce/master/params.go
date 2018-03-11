package master

import (
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Params represents the required parameters to build a Master.
type Params struct {
	RPCParams     rpchandler.Params
	LogMan        util.LogMan
	PingPeriod    time.Duration
	RetryDuration time.Duration
	State         masterstate.State
}

// JobRequest represents job request sent to a master.
type JobRequest struct {
	FilePaths []string
	Type      mrutil.TaskType
}

// JobReply represents the reply to a job request RPC invocation.
type JobReply struct{}

// JobCallResult represents the result of a job request transmission.
type JobCallResult struct {
	request *JobRequest
	errCh   chan error
}

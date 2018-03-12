package master

import (
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Params represents the required parameters to build a Master.
type Params struct {
	RPCParams             rpchandler.Params
	ExpectedWorkerCount   uint
	LogMan                util.LogMan
	PingPeriod            time.Duration
	RetryDuration         time.Duration
	State                 masterstate.State
	WorkerAcceptJobMethod string
	WorkerPingMethod      string
}

// JobReply represents the reply to a job request RPC invocation.
type JobReply struct{}

// JobCallResult represents the result of a job request transmission.
type JobCallResult struct {
	request job.MasterJobRequest
	errCh   chan error
}

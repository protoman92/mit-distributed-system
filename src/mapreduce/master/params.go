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
	Delegate              *MstDelegate
	ExpectedWorkerCount   uint
	LogMan                util.LogMan
	JobRequest            job.MasterJob
	PingPeriod            time.Duration
	RetryDuration         time.Duration
	RPCParams             rpchandler.Params
	RPCHandler            rpchandler.Handler
	State                 masterstate.State
	WorkerAcceptJobMethod string
	WorkerPingMethod      string
}

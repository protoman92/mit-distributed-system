package executor

import (
	wk "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// ExcDelegate represents a delegate for RPC Master to export via rpc.
type ExcDelegate struct {
	shutdownCh chan<- interface{}
	workerCh   chan<- string
}

// Register registers a worker via RPC. This method can be invoked whenever a
// worker has capacity to perform more work, so the master does not need to know
// which worker is free.
func (d *ExcDelegate) Register(args *wk.RegisterParams, reply *wk.RegisterReply) error {
	d.workerCh <- args.WorkerAddress
	return nil
}

// Shutdown performs a shutdown via RPC.
func (d *ExcDelegate) Shutdown(args *ShutdownParams, reply *ShutdownReply) error {
	d.shutdownCh <- true
	return nil
}

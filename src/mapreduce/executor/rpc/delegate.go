package executor

import (
	erpc "github.com/protoman92/mit-distributed-system/src/mapreduce/worker/rpc"
)

// ExcDelegate represents a delegate for RPC Master to export via rpc.
type ExcDelegate struct {
	shutdownCh chan<- interface{}
	workerCh   chan<- string
}

// Register registers a worker via RPC.
func (d *ExcDelegate) Register(args *erpc.RegisterParams, reply *erpc.RegisterReply) error {
	d.workerCh <- args.WorkerAddress
	return nil
}

// Shutdown performs a shutdown via RPC.
func (d *ExcDelegate) Shutdown(args *ShutdownParams, reply *ShutdownReply) error {
	d.shutdownCh <- true
	return nil
}

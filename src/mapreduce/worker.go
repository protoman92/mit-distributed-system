package mapreduce

import (
	"container/list"
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

// Worker is a server waiting for DoJob or Shutdown RPCs
type Worker struct {
	name   string
	Reduce func(string, *list.List) string
	Map    func(string) *list.List
	nRPC   int
	nJobs  int
	l      net.Listener
}

// The master sent us a job
func (wk *Worker) DoJob(arg *util.DoJobArgs, res *util.DoJobReply) error {
	switch arg.Operation {
	case util.Map:
		DoMap(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case util.Reduce:
		DoReduce(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Reduce)
	}
	res.OK = true
	return nil
}

// // The master is telling us to shutdown. Report the number of Jobs we
// // have processed.
// func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
// 	res.Njobs = wk.nJobs
// 	res.OK = true
// 	wk.nRPC = 1 // OK, because the same thread reads nRPC
// 	wk.nJobs--  // Don't count the shutdown RPC
// 	return nil
// }

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, me string,
	MapFunc func(string) *list.List,
	ReduceFunc func(string, *list.List) string, nRPC int) {
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l

	// DON'T MODIFY CODE BELOW
	for wk.nRPC != 0 {
		conn, err := wk.l.Accept()
		if err == nil {
			wk.nRPC -= 1
			go rpcs.ServeConn(conn)
			wk.nJobs += 1
		} else {
			break
		}
	}
	wk.l.Close()
}

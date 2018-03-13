package main

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccess"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/reducer"

	"github.com/protoman92/gocompose/pkg"

	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"

	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate/localstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/util"
)

const (
	mapFunc       = mapper.MapWordCountFn
	mapOpCount    = 3
	masterAddress = "master"
	network       = "unix"
	workerAddress = "worker"
	log           = false
	pingPeriod    = 100e9
	retryCount    = 10
	retryDelay    = time.Duration(1e9)
	reduceFunc    = reducer.ReduceSumFn
	reduceOpCount = 3
	stateLatency  = time.Duration(1e9)
	topN          = 10
	waitTime      = time.Duration(100e9)
	workerCount   = 5
)

var (
	inputFilePaths = make([]string, 0)
	retryFunc      = compose.RetryWithDelay(retryCount)(retryDelay)
)

type fileChunk struct {
	chunks   uint
	filePath string
}

func init() {
	splitAllFiles()
}

func loopMasterError(m master.Master) {
	for {
		select {
		case err := <-m.ErrorChannel():
			fmt.Println("Master error:", reflect.TypeOf(err), ":", err)
		}
	}
}

func loopWorkerError(w worker.Worker) {
	for {
		select {
		case err := <-w.ErrorChannel():
			fmt.Println("Worker error:", reflect.TypeOf(err), ":", err)
		}
	}
}

func main() {
	// go func() {
	// 	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// 	fmt.Println("Single worker word count:")
	// 	countWordsSingle(filePaths)
	// 	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// }()

	os.Remove(masterAddress)
	logMan := util.NewLogMan(util.LogManParams{Log: log})

	mstRPCParams := rpchandler.Params{
		Address:        masterAddress,
		Caller:         rpcutil.NewCaller(),
		LogMan:         logMan,
		Network:        network,
		RetryWithDelay: retryFunc,
	}

	mstDelegate := master.NewDelegate()
	mstRPCHandler := rpchandler.NewHandler(mstRPCParams, mstDelegate)

	master := master.NewMaster(master.Params{
		LogMan:                logMan,
		ExpectedWorkerCount:   10000,
		Delegate:              mstDelegate,
		PingPeriod:            pingPeriod,
		RetryDuration:         1e5,
		WorkerAcceptJobMethod: "WkDelegate.AcceptJob",
		WorkerPingMethod:      "WkDelegate.Ping",
		RPCHandler:            mstRPCHandler,
		RPCParams:             mstRPCParams,
		State: localstate.NewLocalState(localstate.Params{
			Latency: stateLatency,
		}),
	})

	for i := 0; i < workerCount; i++ {
		wkAddress := fmt.Sprintf("%s-%d", workerAddress, i)

		rpcParams := rpchandler.Params{
			Address:        wkAddress,
			Caller:         rpcutil.NewCaller(),
			LogMan:         logMan,
			Network:        network,
			RetryWithDelay: retryFunc,
		}

		os.Remove(wkAddress)
		wkDelegate := worker.NewDelegate()
		rpcHandler := rpchandler.NewHandler(rpcParams, wkDelegate)

		fileAccessor := fileaccess.NewFileAccessor(fileaccess.Params{
			AccessMethod: "WkDelegate.ServeFile",
			Network:      network,
			RPCCaller:    rpcHandler,
		})

		worker := worker.NewWorker(worker.Params{
			Delegate:                wkDelegate,
			LogMan:                  logMan,
			JobCapacity:             1,
			MasterAddress:           masterAddress,
			MasterCompleteJobMethod: "MstDelegate.CompleteJob",
			MasterRegisterMethod:    "MstDelegate.RegisterWorker",
			RPCParams:               rpcParams,
			RPCHandler:              rpcHandler,
			Mapper: mapper.NewMapper(mapper.Params{
				RetryWithDelay: retryFunc,
			}),
			Reducer: reducer.NewReducer(reducer.Params{
				FileAccessor:   fileAccessor,
				RetryWithDelay: retryFunc,
			}),
		})

		go loopWorkerError(worker)
	}

	go loopMasterError(master)
	time.Sleep(time.Second)
	sendJobRequest()

	doneCh := make(chan interface{}, 0)

	go func() {
		for i := 0; i < len(inputFilePaths)*reduceOpCount; i++ {
			<-master.CompletionChannel()
		}

		doneCh <- true
	}()

	select {
	case <-doneCh:
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
		fmt.Println("MapReduce word count:")
		mergeOutputFiles(inputFilePaths)
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	case <-time.After(waitTime):
		fmt.Println("Expired")
	}

	sendShutdownRequest()
	<-master.ShutdownChannel()
}

package main

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/protoman92/gocompose/pkg"

	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"

	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccessor/localaccessor"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate/localstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/util"
)

const (
	mapFunc       = mapper.MapWordCountFn
	mapOpCount    = 10
	masterAddress = "master"
	network       = "unix"
	workerAddress = "worker"
	log           = false
	retryCount    = 10
	retryDelay    = time.Duration(1e9)
	reduceOpCount = 10
	stateLatency  = time.Duration(1e9)
	waitTime      = time.Duration(15e9)
)

func sendJobRequest() {
	fileNames := []string{
		"kjv12.txt",
		// "randomtext.txt",
		// "randomtext2.txt",
	}

	wd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	filePaths := make([]string, 0)
	dir, _ := path.Split(wd)
	fileDir := path.Join(dir, "textinput")

	for ix := range fileNames {
		filePaths = append(filePaths, path.Join(fileDir, fileNames[ix]))
	}

	request := master.JobRequest{
		FilePaths:     filePaths,
		MapFuncName:   mapFunc,
		MapOpCount:    mapOpCount,
		ReduceOpCount: reduceOpCount,
		Type:          mrutil.Map,
	}

	reply := &master.JobReply{}
	master.CheckJobRequest(request)

	callParams := rpcutil.CallParams{
		Args:    request,
		Method:  "MstDelegate.AcceptJob",
		Network: network,
		Reply:   reply,
		Target:  masterAddress,
	}

	if err := rpcutil.Call(callParams); err != nil {
		panic(err)
	}
}

func sendShutdownRequest() {
	if err := rpchandler.Shutdown(network, masterAddress); err != nil {
		panic(err)
	}
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
	os.Remove(masterAddress)
	logMan := util.NewLogMan(util.LogManParams{Log: log})

	master := master.NewMaster(master.Params{
		LogMan:                logMan,
		ExpectedWorkerCount:   10000,
		PingPeriod:            3e9,
		RetryDuration:         1e5,
		WorkerAcceptJobMethod: "WkDelegate.AcceptJob",
		WorkerPingMethod:      "WkDelegate.Ping",
		RPCParams: rpchandler.Params{
			Address:        masterAddress,
			Caller:         rpcutil.NewCaller(),
			LogMan:         logMan,
			Network:        network,
			RetryWithDelay: compose.RetryWithDelay(retryCount)(retryDelay),
		},
		State: localstate.NewLocalState(localstate.Params{
			Latency: stateLatency,
		}),
	})

	for i := 0; i < 5; i++ {
		wkAddress := fmt.Sprintf("%s-%d", workerAddress, i)
		os.Remove(wkAddress)

		worker := worker.NewWorker(worker.Params{
			FileAccessor:            localaccessor.NewLocalFileAccessor(),
			LogMan:                  logMan,
			JobCapacity:             1,
			MasterAddress:           masterAddress,
			MasterCompleteJobMethod: "MstDelegate.CompleteJob",
			MasterRegisterMethod:    "MstDelegate.RegisterWorker",
			RPCParams: rpchandler.Params{
				Address:        wkAddress,
				Caller:         rpcutil.NewCaller(),
				LogMan:         logMan,
				Network:        network,
				RetryWithDelay: compose.RetryWithDelay(retryCount)(retryDelay),
			},
		})

		go loopWorkerError(worker)
	}

	go loopMasterError(master)

	sendJobRequest()
	time.Sleep(waitTime)
	sendShutdownRequest()
	<-master.ShutdownChannel()
}

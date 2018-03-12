package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccessor/localaccessor"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/reducer"

	"github.com/protoman92/gocompose/pkg"

	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"

	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate/localstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
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
	waitTime      = time.Duration(15e9)
	workerCount   = 1
)

var (
	retryFunc = compose.RetryWithDelay(retryCount)(retryDelay)
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

	splitInputFiles(filePaths)

	request := job.MasterJobRequest{
		FilePaths:      filePaths,
		MapFuncName:    mapFunc,
		MapOpCount:     mapOpCount,
		ReduceFuncName: reduceFunc,
		ReduceOpCount:  reduceOpCount,
		Type:           mrutil.Map,
	}

	reply := &master.JobReply{}
	job.CheckMasterJobRequest(request)

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

func splitInputFiles(filePaths []string) {
	for ix := range filePaths {
		fp := filePaths[ix]

		util.SplitFile(fp, mapOpCount, func(chunk uint, data []byte) {
			outputFile := mrutil.MapFileName(fp, chunk)
			file, err := os.Create(outputFile)

			if err != nil {
				panic(err)
			}

			defer file.Close()
			writer := bufio.NewWriter(file)
			writer.Write(data)
		})
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
		PingPeriod:            pingPeriod,
		RetryDuration:         1e5,
		WorkerAcceptJobMethod: "WkDelegate.AcceptJob",
		WorkerPingMethod:      "WkDelegate.Ping",
		RPCParams: rpchandler.Params{
			Address:        masterAddress,
			Caller:         rpcutil.NewCaller(),
			LogMan:         logMan,
			Network:        network,
			RetryWithDelay: retryFunc,
		},
		State: localstate.NewLocalState(localstate.Params{
			Latency: stateLatency,
		}),
	})

	for i := 0; i < workerCount; i++ {
		wkAddress := fmt.Sprintf("%s-%d", workerAddress, i)
		os.Remove(wkAddress)

		worker := worker.NewWorker(worker.Params{
			LogMan:                  logMan,
			JobCapacity:             1,
			MasterAddress:           masterAddress,
			MasterCompleteJobMethod: "MstDelegate.CompleteJob",
			MasterRegisterMethod:    "MstDelegate.RegisterWorker",
			Mapper: mapper.NewMapper(mapper.Params{
				RetryWithDelay: retryFunc,
			}),
			Reducer: reducer.NewReducer(reducer.Params{
				FileAccessor:   localaccessor.NewLocalFileAccessor(),
				RetryWithDelay: retryFunc,
			}),
			RPCParams: rpchandler.Params{
				Address:        wkAddress,
				Caller:         rpcutil.NewCaller(),
				LogMan:         logMan,
				Network:        network,
				RetryWithDelay: retryFunc,
			},
		})

		go loopWorkerError(worker)
	}

	go loopMasterError(master)

	time.Sleep(time.Second)
	sendJobRequest()
	time.Sleep(waitTime)
	sendShutdownRequest()
	<-master.ShutdownChannel()
}

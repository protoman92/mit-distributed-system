package main

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"

	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/util"
)

const (
	masterAddress = "master"
	network       = "unix"
	workerAddress = "worker"
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

	request := &master.JobRequest{FilePaths: filePaths}
	reply := &master.JobReply{}

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
			fmt.Println(reflect.TypeOf(err), ":", err)
		}
	}
}

func loopWorkerError(w worker.Worker) {
	for {
		select {
		case err := <-w.ErrorChannel():
			fmt.Println(reflect.TypeOf(err), ":", err)
		}
	}
}

func main() {
	os.Remove(masterAddress)
	logMan := util.NewLogMan(util.LogManParams{Log: true})

	master := master.NewMaster(master.Params{
		LogMan:     logMan,
		PingPeriod: 3e9,
		RPCParams: rpchandler.Params{
			Address: masterAddress,
			LogMan:  logMan,
			Network: network,
		},
	})

	for i := 0; i < 5; i++ {
		wkAddress := fmt.Sprintf("%s-%d", workerAddress, i)
		os.Remove(wkAddress)

		worker := worker.NewWorker(worker.Params{
			LogMan:               logMan,
			MasterAddress:        masterAddress,
			MasterRegisterMethod: "MstDelegate.RegisterWorker",
			RPCParams: rpchandler.Params{
				Address: wkAddress,
				LogMan:  logMan,
				Network: network,
			},
		})

		go loopWorkerError(worker)
	}

	go loopMasterError(master)

	sendJobRequest()
	time.Sleep(5e9)
	sendShutdownRequest()
	select {}
}

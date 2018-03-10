package main

import (
	"os"
	"path"
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/util"
)

const (
	masterAddress = "master"
	network       = "unix"
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
	request := &master.ShutdownRequest{}
	reply := &master.ShutdownReply{}

	callParams := rpcutil.CallParams{
		Args:    request,
		Method:  "MstDelegate.Shutdown",
		Network: network,
		Reply:   reply,
		Target:  masterAddress,
	}

	if err := rpcutil.Call(callParams); err != nil {
		panic(err)
	}
}

func main() {
	os.Remove(masterAddress)
	logMan := util.NewLogMan(util.LogManParams{Log: true})

	masterParams := master.Params{
		Address: masterAddress,
		LogMan:  logMan,
		Network: network,
	}

	_ = master.NewMaster(masterParams)
	sendJobRequest()
	time.Sleep(2e9)
	sendShutdownRequest()
	select {}
}

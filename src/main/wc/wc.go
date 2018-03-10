package main

import (
	"container/list"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/protoman92/mit-distributed-system/src/util"

	exc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	ir "github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader/local"
	lorc "github.com/protoman92/mit-distributed-system/src/mapreduce/orchestrator/local"
	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter/string"
	wk "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	// panic("No implementation!")
	return list.New()
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	return key
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	fileNames := []string{
		// "kjv12.txt",
		"randomtext.txt",
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

	masterAddress := "master"
	network := "unix"

	// Only applicable for "unix".
	os.Remove(masterAddress)

	logman := util.NewLogMan(util.LogManParams{Log: true})

	oParams := lorc.Params{
		ExecutorParams: exc.Params{
			Address:              masterAddress,
			LogMan:               logman,
			Network:              network,
			WorkerDoJobMethod:    "WkDelegate.DoWork",
			WorkerShutdownMethod: "WkDelegate.Shutdown",
		},
		InputReaderParams: ir.Params{FilePaths: filePaths},
		SplitterParams: sp.Params{
			ChunkCount: 5,
			LogMan:     logman,
			SplitToken: '\n',
		},
		LogMan: logman,
	}

	orchestrator := lorc.NewLocalOrchestrator(oParams)
	workerCount := 5

	for i := 0; i < workerCount; i++ {
		address := "Worker-" + strconv.Itoa(i)

		// Only applicable for "unix".
		os.Remove(address)

		wkParams := wk.Params{
			Address:              address,
			MasterAddress:        masterAddress,
			MasterRegisterMethod: "ExcDelegate.Register",
			Network:              network,
		}

		wk.NewRPCWorker(wkParams)
	}

	doneCh := make(chan interface{}, 1)

	go func() {
		select {
		case <-orchestrator.DoneChannel():
			fmt.Println("Done!")
			doneCh <- true

		case err := <-orchestrator.ErrorChannel():
			fmt.Println(err)
			doneCh <- true
		}
	}()

	<-doneCh
}

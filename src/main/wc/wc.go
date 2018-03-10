package main

import (
	"container/list"
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/inputReader/localReader"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper/localMapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/orchestrator/localOrc"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/splitter/stringSplitter"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/util"
)

var (
	rgx = regexp.MustCompile("[^a-zA-Z0-9 ]+")
)

// MapFunc for mapper - count word occurrence for a piece of string.
func MapFunc(chunk *mrutil.DataChunk) error {
	value := chunk.ValueString()
	wordMap := make(map[string]int, 0)
	lines := strings.Split(value, "\n")
	var wordMutex sync.RWMutex
	var waitGroup sync.WaitGroup

	addWordCount := func(words string) {
		cleaned := rgx.ReplaceAllString(words, "")
		delimited := strings.Split(cleaned, " ")

		for ix := range delimited {
			word := delimited[ix]
			wordMutex.Lock()
			wordMap[word] = wordMap[word] + 1
			wordMutex.Unlock()
		}
	}

	getWordMap := func() map[string]int {
		wordMutex.RLock()
		defer wordMutex.RUnlock()
		return wordMap
	}

	for ix := range lines {
		waitGroup.Add(1)

		go func(words string) {
			addWordCount(words)
			waitGroup.Done()
		}(lines[ix])
	}

	waitGroup.Wait()
	fmt.Println(getWordMap())
	return nil
}

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

	masterAddress := "master"
	network := "unix"

	// Only applicable for "unix".
	os.Remove(masterAddress)

	logman := util.NewLogMan(util.LogManParams{Log: false})

	oParams := localOrc.Params{
		ExecutorParams: executor.Params{
			Address:              masterAddress,
			LogMan:               logman,
			Network:              network,
			WorkerDoJobMethod:    "WkDelegate.DoWork",
			WorkerShutdownMethod: "WkDelegate.Shutdown",
		},
		InputReaderParams: localReader.Params{
			SplitterParams: stringSplitter.Params{
				ChunkCount: 10,
				LogMan:     logman,
				SplitToken: '\n',
			},
			FilePaths: filePaths,
		},
		LogMan: logman,
	}

	orchestrator := localOrc.NewLocalOrchestrator(oParams)
	workerCount := 5

	for i := 0; i < workerCount; i++ {
		address := "Worker-" + strconv.Itoa(i)

		// Only applicable for "unix".
		os.Remove(address)

		mapper := localMapper.NewLocalMapper(localMapper.Params{MapFunc: MapFunc})

		wkParams := worker.Params{
			Address:              address,
			LogMan:               logman,
			Mapper:               mapper,
			MasterAddress:        masterAddress,
			MasterRegisterMethod: "ExcDelegate.Register",
			Network:              network,
		}

		worker.NewRPCWorker(wkParams)
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

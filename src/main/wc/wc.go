package main

import (
	"container/list"
	"fmt"
	"os"
	"path"

	sp "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/splitter"

	rw "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/readWriter"

	exc "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/executor"
	orc "github.com/protoman92/mit-distributed-system-6.824/src/mapreduce/orchestrator"
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
	filename := "kjv12.txt"
	wd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	dir, _ := path.Split(wd)
	fileDir := path.Join(dir, "kjv12")

	params := orc.LocalParams{
		ExecutorParams:   exc.Params{},
		ReadWriterParams: rw.LocalParams{FileName: filename, FileDir: fileDir},
		SplitterParams:   sp.StringParams{ChunkCount: 5, SplitToken: '\n'},
	}

	orchestrator := orc.NewLocalOrchestrator(params)
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

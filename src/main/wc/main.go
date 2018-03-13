package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/protoman92/gocompose/pkg"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccess"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate/localstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/reducer"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
	"github.com/protoman92/mit-distributed-system/src/util"
)

const (
	masterAddress = "master"
	network       = "unix"
	workerAddress = "worker"
)

const (
	mapFunc       = mapper.MapWordCountFn
	mapOpCount    = 8
	reduceFunc    = reducer.ReduceSumFn
	reduceOpCount = 8
)

const (
	log            = false
	pingPeriod     = 100e9
	retryCount     = 5
	retryDelay     = time.Duration(1e9)
	stateLatency   = time.Duration(1e9)
	topN           = 100
	waitTime       = time.Duration(100e9)
	workerCapacity = 10
	workerCount    = 5
)

var (
	inputFilePaths = make([]string, 0)
	retryFunc      = compose.RetryWithDelay(retryCount)(retryDelay)
)

type fileChunk struct {
	chunks   uint
	filePath string
}

func main() {
	splitAllFiles()

	// go func() {
	// 	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// 	fmt.Println("Single worker word count:")
	// 	countWordsSingle(filePaths)
	// 	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// }()

	os.Remove(masterAddress)
	logMan := util.NewLogMan(util.LogManParams{Log: log})
	start := time.Now()

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
		JobRequest: job.MasterJob{
			FilePaths:      inputFilePaths,
			MapFuncName:    mapFunc,
			MapOpCount:     mapOpCount,
			ReduceFuncName: reduceFunc,
			ReduceOpCount:  reduceOpCount,
			Type:           mrutil.Map,
		},
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

		_ = worker.NewWorker(worker.Params{
			Delegate:                wkDelegate,
			LogMan:                  logMan,
			JobCapacity:             workerCapacity,
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
	}

	time.Sleep(time.Second)

	select {
	case <-master.AllCompletedChannel():
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
		fmt.Println("MapReduce word count:")
		mergeOutputFiles(inputFilePaths)
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	case <-time.After(waitTime):
		fmt.Println("Expired")
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("Elapsed: %ds\n", elapsed/1000000000)
	sendShutdownRequest()
	<-master.ShutdownChannel()
	cleanupFiles(inputFilePaths)
}

func splitAllFiles() {
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

	fileChunks := splitInputFiles(
		filePaths,
		func(fp string, callback func(uint, []byte) error) error {
			return util.SplitFile(fp, mapOpCount, callback)
		},
		mrutil.SplitFileName,
	)

	for ix := range fileChunks {
		fc := fileChunks[ix]

		for i := 0; i < int(fc.chunks); i++ {
			splitFP := mrutil.SplitFileName(fc.filePath, uint(i))
			inputFilePaths = append(inputFilePaths, splitFP)
		}
	}

	// Split each portion into Map files as well. So in total, we have S split
	// files * M map files.
	_ = splitInputFiles(
		inputFilePaths,
		func(fp string, callback func(uint, []byte) error) error {
			return util.SplitFile(fp, mapOpCount, callback)
		},
		mrutil.MapFileName,
	)
}

func splitInputFiles(
	filePaths []string,
	splitFn func(string, func(uint, []byte) error) error,
	splitNameFn func(string, uint) string,
) []fileChunk {
	mutex := sync.Mutex{}
	fileChunks := make([]fileChunk, 0)
	waitGroup := sync.WaitGroup{}

	appendFileChunk := func(chunk fileChunk) {
		mutex.Lock()
		defer mutex.Unlock()
		fileChunks = append(fileChunks, chunk)
	}

	for ix := range filePaths {
		waitGroup.Add(1)

		go func(fp string) {
			defer waitGroup.Done()

			if err := splitFn(fp, func(chunk uint, data []byte) error {
				outputFile := splitNameFn(fp, chunk)
				file, err := os.Create(outputFile)

				if err != nil {
					return err
				}

				defer file.Close()
				writer := bufio.NewWriter(file)
				_, err1 := writer.Write(data)
				return err1
			}); err != nil {
				panic(err)
			}

			newChunk := fileChunk{filePath: fp, chunks: mapOpCount}
			appendFileChunk(newChunk)
		}(filePaths[ix])
	}

	waitGroup.Wait()
	return fileChunks
}

func countWordsSingle(filePaths []string) {
	kvm := make(map[string]int, 0)

	for ix := range filePaths {
		file, err := os.Open(filePaths[ix])

		if err != nil {
			panic(err)
		}

		contents, _ := ioutil.ReadAll(file)

		if err != nil {
			file.Close()
			panic(err)
		}

		file.Close()

		words := strings.FieldsFunc(string(contents), func(r rune) bool {
			return !unicode.IsDigit(r) && !unicode.IsLetter(r)
		})

		for jx := range words {
			word := words[jx]
			kvm[word] = kvm[word] + 1
		}
	}

	rankWordCount(kvm)
}

func mergeOutputFiles(filePaths []string) {
	kvm := make(map[string]int, 0)

	for ix := range filePaths {
		for i := 0; i < reduceOpCount; i++ {
			fp := mrutil.MergeFileName(filePaths[ix], uint(i))
			file, err := os.Open(fp)

			if err != nil {
				panic(err)
			}

			decoder := json.NewDecoder(file)
			line := 0

			for {
				var kv mrutil.KeyValue
				err := decoder.Decode(&kv)
				line++

				if err != nil {
					if err != io.EOF {
						fmt.Println(fp, line)
						panic(err)
					} else {
						break
					}
				}

				value, err := strconv.Atoi(kv.Value)

				if err != nil {
					panic(err)
				}

				kvm[kv.Key] = kvm[kv.Key] + value
			}
		}
	}

	rankWordCount(kvm)
}

func cleanupFiles(filePaths []string) {
	fmt.Println("Removing", filePaths)

	for ix := range filePaths {
		splitFP := filePaths[ix]

		if err := os.Remove(splitFP); err != nil {
			fmt.Println(err)
		}

		for i := 0; i < mapOpCount; i++ {
			mapFP := mrutil.MapFileName(splitFP, uint(i))

			if err := os.Remove(mapFP); err != nil {
				fmt.Println(err)
			}

			for j := 0; j < reduceOpCount; j++ {
				reduceFP := mrutil.ReduceFileName(splitFP, uint(i), uint(j))

				if err := os.Remove(reduceFP); err != nil {
					fmt.Println(err)
				}
			}

			mergedFP := mrutil.MergeFileName(splitFP, uint(i))

			if err := os.Remove(mergedFP); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func rankWordCount(kvm map[string]int) {
	vkm := make(map[int][]string, 0)

	for key := range kvm {
		value := kvm[key]
		keys, ok := vkm[value]

		if !ok {
			keys = make([]string, 0)
		}

		keys = append(keys, key)
		vkm[value] = keys
	}

	values := make([]int, 0)

	for k := range vkm {
		values = append(values, k)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(values)))
	currentTop := 0
	currentIndex := 0
	topKV := make([]mrutil.KeyValue, 0)

	for currentTop < topN {
		value := values[currentIndex]
		keys := vkm[value]

		for ix := range keys {
			currentTop++
			kv := mrutil.KeyValue{Key: keys[ix], Value: strconv.Itoa(value)}
			topKV = append(topKV, kv)
		}

		currentIndex++
	}

	for ix := range topKV {
		fmt.Printf("Number %d: %v\n", ix+1, topKV[ix])
	}

	sum := 0

	for ix := range values {
		sum += values[ix]
	}

	fmt.Printf("Total word count: %d\n", sum)
}

func sendShutdownRequest() {
	if err := rpchandler.Shutdown(network, masterAddress); err != nil {
		panic(err)
	}
}

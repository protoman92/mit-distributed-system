package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

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
	topN          = 10
	waitTime      = time.Duration(15e9)
	workerCount   = 5
)

var (
	filePaths []string
	retryFunc = compose.RetryWithDelay(retryCount)(retryDelay)
)

func init() {
	fileNames := []string{
		"kjv12.txt",
		// "randomtext.txt",
		// "randomtext2.txt",
	}

	wd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	filePaths = make([]string, 0)
	dir, _ := path.Split(wd)
	fileDir := path.Join(dir, "textinput")

	for ix := range fileNames {
		filePaths = append(filePaths, path.Join(fileDir, fileNames[ix]))
	}

	splitInputFiles(filePaths)
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

			for {
				var kv mrutil.KeyValue
				err := decoder.Decode(&kv)

				if err != nil {
					if err != io.EOF {
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

	fmt.Printf("Total word count: %d", sum)
}

func sendJobRequest() {
	request := job.MasterJob{
		FilePaths:      filePaths,
		MapFuncName:    mapFunc,
		MapOpCount:     mapOpCount,
		ReduceFuncName: reduceFunc,
		ReduceOpCount:  reduceOpCount,
		Type:           mrutil.Map,
	}

	reply := &master.JobReply{}
	job.CheckMasterJob(request)

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

		if err := util.SplitFile(fp, mapOpCount, func(chunk uint, data []byte) error {
			outputFile := mrutil.MapFileName(fp, chunk)
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
	// go func() {
	// 	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// 	fmt.Println("Single worker word count:")
	// 	countWordsSingle(filePaths)
	// 	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// }()

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

	doneCh := make(chan interface{}, 0)

	go func() {
		for i := 0; i < len(filePaths)*reduceOpCount; i++ {
			<-master.CompletionChannel()
		}

		doneCh <- true
	}()

	select {
	case <-doneCh:
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
		fmt.Println("MapReduce word count:")
		mergeOutputFiles(filePaths)
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	case <-time.After(waitTime):
		fmt.Println("Expired")
	}

	sendShutdownRequest()
	<-master.ShutdownChannel()
}

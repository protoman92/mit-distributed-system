package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/master"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
)

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
		FilePaths:      inputFilePaths,
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

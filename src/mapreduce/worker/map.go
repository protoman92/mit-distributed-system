package worker

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

func (w *worker) doMap(r JobRequest) error {
	file, err := os.Open(r.FilePath)

	if err != nil {
		return err
	}

	defer file.Close()
	fInfo, err := file.Stat()

	if err != nil {
		return err
	}

	size := fInfo.Size()
	chunkSize := size/int64(r.MapOpCount) + 1
	intermediate := ""
	intermediateSize := int64(0)
	scanner := bufio.NewScanner(file)
	mapErrorCh := make(chan error, r.MapOpCount)
	waitGroup := sync.WaitGroup{}

	processIntermediate := func(intermediate string) {
		waitGroup.Add(1)

		go func() {
			results := w.mapFunction(r)(r.FilePath, []byte(intermediate))

			handleMapResults := func() error {
				return w.handleMapResults(r, results)
			}

			if err := w.RPCParams.RetryWithDelay(handleMapResults)(); err != nil {
				mapErrorCh <- err
			}

			waitGroup.Done()
		}()
	}

	for scanner.Scan() {
		text := scanner.Text()
		intermediateSize += int64(len(text))
		intermediate = strings.Join([]string{intermediate, text}, "\n")

		if intermediateSize >= chunkSize {
			processIntermediate(intermediate)
			intermediate = ""
			intermediateSize = 0
		}
	}

	processIntermediate(intermediate)
	waitGroup.Wait()

	select {
	case err := <-mapErrorCh:
		return err

	default:
		return nil
	}
}

func (w *worker) mapFunction(r JobRequest) mapper.MapFunc {
	switch r.MapFuncName {
	case mapper.MapWordCountFn:
		return mapper.MapWordCount

	case mapper.MapNoopFn:
		return mapper.MapNoop

	default:
		panic("Unsupported operation")
	}
}

func (w *worker) handleMapResults(r JobRequest, kvs []mrutil.KeyValue) error {
	files := make([]*os.File, 0)
	encoders := make([]*json.Encoder, 0)

	defer func() {
		for ix := range files {
			files[ix].Close()
		}
	}()

	for i := 0; i < int(r.ReduceOpCount); i++ {
		fName := w.reduceFilePath(r.FilePath, i)
		file, err := os.Create(fName)

		if err != nil {
			return err
		}

		encoder := json.NewEncoder(file)
		files = append(files, file)
		encoders = append(encoders, encoder)
	}

	for ix := range kvs {
		kv := kvs[ix]
		fileIndex := int(w.hash(kv.Key) % uint32(r.ReduceOpCount))
		encoder := encoders[fileIndex]

		if err := encoder.Encode(kv); err != nil {
			return err
		}
	}

	return nil
}

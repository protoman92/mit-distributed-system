package worker

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

func (w *worker) doMap(r *JobRequest) error {
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
	chunkSize := size / int64(r.MapOpCount)
	intermediate := make([][]byte, 0)
	intermediateSize := int64(0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		bytes := scanner.Bytes()
		intermediateSize += int64(len(bytes))
		intermediate = append(intermediate, bytes)

		if intermediateSize >= chunkSize {
			go func() {
				w.handleMapResults(r, w.mapFunction(r)(r.FilePath, intermediate))
			}()

			intermediate = make([][]byte, 0)
			intermediateSize = 0
		}
	}

	return nil
}

func (w *worker) mapFunction(r *JobRequest) mapper.MapFunc {
	switch r.MapFuncName {
	case mapper.MapWordCountFn:
		return mapper.MapWordCount

	default:
		panic("Unsupported operation")
	}
}

func (w *worker) handleMapResults(r *JobRequest, kvs []*mrutil.KeyValue) {
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
			// Retry repeatedly.
			w.handleMapResults(r, kvs)
			return
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
			// Retry repeatedly.
			w.handleMapResults(r, kvs)
			return
		}
	}
}

package mapper

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Mapper represents a Map performer.
type Mapper interface {
	DoMap(r job.WorkerJobRequest) error
}

type mapPerformer struct {
	*Params
}

func (m *mapPerformer) hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (m *mapPerformer) DoMap(r job.WorkerJobRequest) error {
	mapErrorCh := make(chan error, r.MapOpCount)
	doneCh := make(chan interface{}, 0)
	waitGroup := sync.WaitGroup{}

	for i := 0; i < int(r.MapOpCount); i++ {
		go func(mapNo uint) {
			fp := mrutil.MapFileName(r.FilePath, mapNo)

			if err := util.SplitFile(fp, r.ReduceOpCount, func(chunk uint, data []byte) {
				waitGroup.Add(1)

				go func() {
					results := m.mapFunction(r)(r.FilePath, data)

					handleMapResults := func() error {
						return m.handleMapResults(r, mapNo, results)
					}

					if err := m.RetryWithDelay(handleMapResults)(); err != nil {
						mapErrorCh <- err
					}

					waitGroup.Done()
				}()
			}); err != nil {
				mapErrorCh <- err
			}
		}(uint(i))
	}

	go func() {
		waitGroup.Wait()
		doneCh <- true
	}()

	select {
	case err := <-mapErrorCh:
		return err

	case <-doneCh:
		return nil
	}
}

func (m *mapPerformer) mapFunction(r job.WorkerJobRequest) mrutil.MapFunc {
	switch r.MapFuncName {
	case MapWordCountFn:
		return MapWordCount

	case MapNoopFn:
		return MapNoop

	default:
		panic("Unsupported operation")
	}
}

func (m *mapPerformer) handleMapResults(r job.WorkerJobRequest, mapNo uint, kvs []mrutil.KeyValue) error {
	files := make([]*os.File, 0)
	encoders := make([]*json.Encoder, 0)

	defer func() {
		for ix := range files {
			files[ix].Close()
		}
	}()

	for i := 0; i < int(r.ReduceOpCount); i++ {
		fName := mrutil.ReduceFileName(r.FilePath, mapNo, uint(i))
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
		fileIndex := int(m.hash(kv.Key) % uint32(r.ReduceOpCount))
		encoder := encoders[fileIndex]

		if err := encoder.Encode(kv); err != nil {
			return err
		}
	}

	return nil
}

// NewMapper returns a new Mapper.
func NewMapper(params Params) Mapper {
	checked := checkParams(&params)
	return &mapPerformer{Params: checked}
}

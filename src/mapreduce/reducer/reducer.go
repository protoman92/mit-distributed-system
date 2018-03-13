package reducer

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccess"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// Reducer represents an object that can perform Reduce.
type Reducer interface {
	DoReduce(r job.WorkerJob) error
}

type reducePerformer struct {
	*Params
}

// For Reduce, we may need to access remote files in another worker. The file
// path included in the job request should have been formatted to include
// URI information, and we need a special file accessor to retrieve the data
// at this URI.
func (rd *reducePerformer) DoReduce(r job.WorkerJob) error {
	doneCh := make(chan interface{}, 0)
	reduceErrCh := make(chan error, 0)
	waitGroup := sync.WaitGroup{}

	mergeFP := mrutil.MergeFileName(r.File, r.JobNumber)
	mergeFile, err := os.Create(mergeFP)

	if err != nil {
		return err
	}

	defer mergeFile.Close()
	encoder := json.NewEncoder(mergeFile)

	// Search for M files. These files may be on different localities, so the
	// worst case scenario is that this worker has to make M network calls.
	for i := 0; i < int(r.MapOpCount); i++ {
		mapNo := uint(i)
		waitGroup.Add(1)

		go func() {
			cb := func(data []byte) error {
				kvm, err := rd.mapKeyFromInput(r, data)

				if err != nil && err != io.EOF {
					return err
				}

				results := make([]mrutil.KeyValue, 0)
				reduceFn := rd.reduceFunction(r)

				for key := range kvm {
					reduced := reduceFn(key, kvm[key])
					results = append(results, reduced)
				}

				for ix := range results {
					if err := encoder.Encode(&results[ix]); err != nil {
						return err
					}
				}

				return nil
			}

			accessParams := fileaccess.AccessParams{
				Address: r.RemoteFileAddr,
				File:    mrutil.ReduceFileName(r.File, mapNo, r.JobNumber),
			}

			if err := rd.FileAccessor.AccessFile(accessParams, cb); err != nil {
				reduceErrCh <- err
			} else {
				waitGroup.Done()
			}
		}()
	}

	go func() {
		waitGroup.Wait()
		doneCh <- true
	}()

	select {
	case err := <-reduceErrCh:
		return err

	case <-doneCh:
		return nil
	}
}

func (rd *reducePerformer) mapKeyFromInput(r job.WorkerJob, data []byte) (map[string][]string, error) {
	kvMap := make(map[string][]string, 0)
	buffer := bytes.NewBuffer(data)
	decoder := json.NewDecoder(buffer)

	for {
		var kv mrutil.KeyValue
		err := decoder.Decode(&kv)

		if err != nil {
			return kvMap, err
		}

		values, ok := kvMap[kv.Key]

		if !ok {
			values = make([]string, 0)
		}

		values = append(values, kv.Value)
		kvMap[kv.Key] = values
	}
}

func (rd *reducePerformer) reduceFunction(r job.WorkerJob) mrutil.ReduceFunc {
	switch r.ReduceFuncName {
	case ReduceSumFn:
		return ReduceSum

	default:
		panic("Unsupported operation")
	}
}

// NewReducer returns a new Reducer.
func NewReducer(params Params) Reducer {
	checked := checkParams(&params)
	return &reducePerformer{Params: checked}
}

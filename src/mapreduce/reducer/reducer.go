package reducer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// Reducer represents an object that can perform Reduce.
type Reducer interface {
	DoReduce(r job.WorkerJobRequest) error
}

type reducePerformer struct {
	*Params
}

// For Reduce, we may need to access remote files in another worker. The file
// path included in the job request should have been formatted to include
// URI information, and we need a special file accessor to retrieve the data
// at this URI.
func (rd *reducePerformer) DoReduce(r job.WorkerJobRequest) error {
	kvMap := make(map[string][]string, 0)

	for i := 0; i < int(r.MapOpCount); i++ {
		for j := 0; j < int(r.ReduceOpCount); j++ {
			inputPath := mrutil.ReduceFileName(r.FilePath, uint(i), uint(j))

			if err := rd.FileAccessor.AccessFile(inputPath, func(data []byte) error {
				kvm, err := rd.mapKeyFromInput(r, data)

				if err != nil && err != io.EOF {
					return err
				}

				for key := range kvm {
					values, ok := kvMap[key]

					if !ok {
						values = make([]string, 0)
					}

					values = append(values, kvm[key]...)
					kvMap[key] = values
				}

				return nil
			}); err != nil {
				return err
			}
		}
	}

	results := make([]mrutil.KeyValue, 0)
	reduceFn := rd.reduceFunction(r)

	for key := range kvMap {
		reduced := reduceFn(key, kvMap[key])
		results = append(results, reduced)
	}

	fmt.Println(results)
	return nil
}

func (rd *reducePerformer) mapKeyFromInput(r job.WorkerJobRequest, data []byte) (map[string][]string, error) {
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

func (rd *reducePerformer) reduceFunction(r job.WorkerJobRequest) mrutil.ReduceFunc {
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

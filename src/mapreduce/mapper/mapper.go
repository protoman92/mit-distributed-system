package mapper

import (
	"encoding/json"
	"hash/fnv"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Mapper represents a Map performer.
type Mapper interface {
	DoMap(r job.WorkerJob) error
}

type mapPerformer struct {
	*Params
}

func (m *mapPerformer) hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (m *mapPerformer) DoMap(r job.WorkerJob) error {
	files := make([]*os.File, 0)
	encoders := make([]*json.Encoder, 0)

	defer func() {
		for ix := range files {
			files[ix].Close()
		}
	}()

	for i := 0; i < int(r.ReduceOpCount); i++ {
		fName := mrutil.ReduceFileName(r.File, r.JobNumber, uint(i))
		file, err := os.Create(fName)

		if err != nil {
			return err
		}

		encoder := json.NewEncoder(file)
		files = append(files, file)
		encoders = append(encoders, encoder)
	}

	fp := mrutil.MapFileName(r.File, r.JobNumber)

	// Split into R files and write intermediate data.
	return util.SplitFile(fp, r.ReduceOpCount, func(chunk uint, data []byte) error {
		results := m.mapFunction(r)(r.File, data)

		for ix := range results {
			kv := results[ix]
			fileIndex := int(m.hash(kv.Key) % uint32(r.ReduceOpCount))
			encoder := encoders[fileIndex]

			if err := encoder.Encode(&kv); err != nil {
				return err
			}
		}

		return nil
	})
}

func (m *mapPerformer) mapFunction(r job.WorkerJob) mrutil.MapFunc {
	switch r.MapFuncName {
	case MapWordCountFn:
		return MapWordCount

	case MapNoopFn:
		return MapNoop

	default:
		panic("Unsupported operation")
	}
}

// NewMapper returns a new Mapper.
func NewMapper(params Params) Mapper {
	checked := checkParams(&params)
	return &mapPerformer{Params: checked}
}

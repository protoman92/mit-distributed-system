package mapreduce

import (
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
)

// KeyValue represents a tuple of key-value.
type KeyValue struct {
	Key   string
	Value string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// DoMap read split for job, call Map for that split, and create nreduce
// partitions.
func DoMap(JobNumber int, fileName string, nreduce int, Map func(string) *list.List) {
	name := MapName(fileName, JobNumber)
	file, err := os.Open(name)

	if err != nil {
		log.Fatal("DoMap: ", err)
	}

	fi, err := file.Stat()

	if err != nil {
		log.Fatal("DoMap: ", err)
	}

	size := fi.Size()
	fmt.Printf("DoMap: read split %s %d\n", name, size)
	b := make([]byte, size)
	_, err = file.Read(b)

	if err != nil {
		log.Fatal("DoMap: ", err)
	}

	file.Close()
	res := Map(string(b))

	// XXX a bit inefficient. could open r files and run over list once
	for r := 0; r < nreduce; r++ {
		file, err = os.Create(ReduceName(fileName, JobNumber, r))

		if err != nil {
			log.Fatal("DoMap: create ", err)
		}

		enc := json.NewEncoder(file)

		for e := res.Front(); e != nil; e = e.Next() {
			kv := e.Value.(KeyValue)

			if hash(kv.Key)%uint32(nreduce) == uint32(r) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("DoMap: marshall ", err)
				}
			}
		}

		file.Close()
	}
}

// DoReduce reads map outputs for partition job, sort them by key, call reduce
// for each key
func DoReduce(job int, fileName string, nmap int, Reduce func(string, *list.List) string) {
	kvs := make(map[string]*list.List)

	for i := 0; i < nmap; i++ {
		name := ReduceName(fileName, i, job)
		fmt.Printf("DoReduce: read %s\n", name)
		file, err := os.Open(name)

		if err != nil {
			log.Fatal("DoReduce: ", err)
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = list.New()
			}
			kvs[kv.Key].PushBack(kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p := MergeName(fileName, job)
	file, err := os.Create(p)
	if err != nil {
		log.Fatal("DoReduce: create ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := Reduce(k, kvs[k])
		enc.Encode(KeyValue{k, res})
	}
	file.Close()
}

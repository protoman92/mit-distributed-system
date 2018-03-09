package util

import (
	"fmt"
)

const (
	// Map is the first part of a MapReduce process.
	Map JobType = "Map"

	// Reduce is the second part of a MapReduce process.
	Reduce JobType = "Reduce"
)

// JobType represents the type of job that should be performed. Depending on
// the type, a worker may perform different tasks.
type JobType string

// KeyValue represents a key-value pair for a Map process. For e.g., this can
// be document name - document contents.
type KeyValue struct {
	Key   string
	Value []byte
}

func (kv *KeyValue) String() string {
	return fmt.Sprintf("Key %s, value count %d", kv.Key, len(kv.Value))
}

// KeyValueSize is similar to KeyValue, but includes a total size value so that
// we can calculate the number of chunks which, combined together, would contain
// all data for a certain key.
type KeyValueSize struct {
	*KeyValue
	TotalSize int64
}

func (kvs *KeyValueSize) String() string {
	return fmt.Sprintf("%v, with total size %d", kvs.KeyValue, kvs.TotalSize)
}

// KeyValuePipe defines a pipe of data for a key.
type KeyValuePipe struct {
	Key       string
	ValueCh   <-chan []byte
	TotalSize int64
}

// RPC arguments and replies.  Field names must start with capital letters,
// otherwise RPC will break.
type DoJobArgs struct {
	File          string
	Operation     JobType
	JobNumber     int // this job's number
	NumOtherPhase int // total number of jobs in other phase (map or reduce)
}

type DoJobReply struct {
	OK bool
}

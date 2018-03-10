package mrutil

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

// DataChunk represents chunk of contents for a certain key.
type DataChunk struct {
	Key    string
	Value  []byte
	NChunk uint
}

func (dc *DataChunk) String() string {
	return fmt.Sprintf("Key %s, chunk %d, value count %d", dc.Key, dc.NChunk, len(dc.Value))
}

// ValueString returns the string representation of the value.
func (dc *DataChunk) ValueString() string {
	return string(dc.Value)
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

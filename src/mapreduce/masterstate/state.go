package masterstate

import "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"

// State represents a master's mutable state, in which it keeps task information.
// A State object abstracts away key-value get/set implementations, so we can
// have local State or remote State with database access.
type State interface {
	IdleTasks() ([]*worker.Task, error)
	RegisterTasks(tasks ...*worker.Task) error
}

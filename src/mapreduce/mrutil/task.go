package mrutil

// Task represents a Map/Reduce task.
type Task struct {
	Status Status
	Type   Type
	Worker string
}

package mrutil

import "fmt"

func (kv *KeyValue) String() string {
	return fmt.Sprintf("Key %s, value length %d", kv.Key, len(kv.Value))
}

func (t TaskStatus) String() string {
	switch t {
	case Idle:
		return "Idle"

	case InProgress:
		return "In-Progress"

	case Completed:
		return "Completed"

	default:
		panic("Invalid task status")
	}
}

func (t TaskType) String() string {
	switch t {
	case Map:
		return "Map"

	case Reduce:
		return "Reduce"

	default:
		panic("Invalid task type")
	}
}

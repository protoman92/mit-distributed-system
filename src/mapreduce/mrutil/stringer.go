package mrutil

import (
	"fmt"
	"strconv"
)

func (kv *KeyValue) String() string {
	return fmt.Sprintf("Key %s, value length %d", kv.Key, len(kv.Value))
}

func (t JobStatus) String() string {
	switch t {
	case Idle:
		return "Idle"

	case InProgress:
		return "In-Progress"

	case Completed:
		return "Completed"

	default:
		return strconv.Itoa(int(t))
	}
}

func (t JobType) String() string {
	switch t {
	case Map:
		return "Map"

	case Reduce:
		return "Reduce"

	default:
		return strconv.Itoa(int(t))
	}
}

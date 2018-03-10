package worker

import (
	"fmt"
)

func (w *servant) String() string {
	return fmt.Sprintf("Worker %s", w.Address)
}

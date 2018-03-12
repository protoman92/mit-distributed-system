package worker

import (
	"fmt"
)

func (w *worker) String() string {
	return fmt.Sprintf("Worker %s", w.RPCParams.Address)
}

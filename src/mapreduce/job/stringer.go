package job

import (
	"fmt"
	"path"
)

func (r WorkerJob) String() string {
	_, name := path.Split(r.File)
	return fmt.Sprintf("File %s, type %s, map number %d", name, r.Type, r.MapJobNumber)
}

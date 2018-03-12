package worker

import (
	"fmt"
	"path"
)

func (w *worker) reduceFilePath(filePath string, jobNumber int) string {
	dir, file := path.Split(filePath)
	newName := fmt.Sprintf("reduce-%d-%s", jobNumber, file)
	return path.Join(dir, newName)
}

func (w *worker) doReduce(r JobRequest) error {
	return nil
}

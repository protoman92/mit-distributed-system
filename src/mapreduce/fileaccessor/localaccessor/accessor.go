package localaccessor

import (
	"bufio"
	"io"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccessor"
)

type localAccessor struct{}

func (a *localAccessor) AccessFile(uri string, fn func([]byte) error) error {
	file, err := os.Open(uri)

	if err != nil {
		return err
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		if err := fn(scanner.Bytes()); err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

// NewLocalFileAccessor returns a new LocalFileAccessor.
func NewLocalFileAccessor() fileaccessor.FileAccessor {
	accessor := &localAccessor{}
	return accessor
}

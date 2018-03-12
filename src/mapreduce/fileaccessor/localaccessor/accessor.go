package localaccessor

import (
	"bufio"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccessor"
)

type localAccessor struct{}

func (a *localAccessor) FormatURI(path string) string {
	return path
}

func (a *localAccessor) AccessFile(uri string, fn func([]byte)) error {
	file, err := os.Open(uri)

	if err != nil {
		return err
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		fn(scanner.Bytes())
	}

	return nil
}

// NewLocalFileAccessor returns a new LocalFileAccessor.
func NewLocalFileAccessor() fileaccessor.FileAccessor {
	accessor := &localAccessor{}
	return accessor
}

package util

import (
	"bufio"
	"os"
	"strings"
)

// SplitFile splits a files into multiple parts, based on the number of chunks
// specified.
func SplitFile(filePath string, chunks uint, callback func(uint, []byte)) error {
	file, err := os.Open(filePath)

	if err != nil {
		return err
	}

	defer file.Close()
	fInfo, err := file.Stat()

	if err != nil {
		return err
	}

	size := fInfo.Size()
	currentChunk := uint(0)
	chunkSize := size/int64(chunks) + 1
	intermediate := ""
	intermediateSize := int64(0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := scanner.Text()
		intermediateSize += int64(len(text))
		intermediate = strings.Join([]string{intermediate, text}, "\n")

		if intermediateSize >= chunkSize {
			callback(currentChunk, []byte(intermediate))
			currentChunk++
			intermediate = ""
			intermediateSize = 0
		}
	}

	if len(intermediate) > 0 {
		callback(currentChunk, []byte(intermediate))
	}

	return nil
}

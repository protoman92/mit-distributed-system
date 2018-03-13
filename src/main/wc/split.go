package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/util"
)

func splitAllFiles() {
	fileNames := []string{
		"kjv12.txt",
		// "randomtext.txt",
		// "randomtext2.txt",
	}

	wd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	filePaths := make([]string, 0)
	dir, _ := path.Split(wd)
	fileDir := path.Join(dir, "textinput")

	for ix := range fileNames {
		filePaths = append(filePaths, path.Join(fileDir, fileNames[ix]))
	}

	fileChunks := splitInputFiles(
		filePaths,
		func(fp string, callback func(uint, []byte) error) error {
			return util.SplitFile(fp, mapOpCount, callback)
		},
		splitFilePath,
	)

	for ix := range fileChunks {
		fc := fileChunks[ix]

		for i := 0; i < int(fc.chunks); i++ {
			splitFP := splitFilePath(fc.filePath, uint(i))
			inputFilePaths = append(inputFilePaths, splitFP)
		}
	}

	// Split each portion into Map files as well. So in total, we have S split
	// files * M map files.
	_ = splitInputFiles(
		inputFilePaths,
		func(fp string, callback func(uint, []byte) error) error {
			return util.SplitFile(fp, mapOpCount, callback)
		},
		mrutil.MapFileName,
	)
}

func splitFilePath(fp string, chunk uint) string {
	dir, name := path.Split(fp)
	splitFP := fmt.Sprintf("%d-%s", chunk, name)
	return path.Join(dir, splitFP)
}

func splitInputFiles(
	filePaths []string,
	splitFn func(string, func(uint, []byte) error) error,
	splitNameFn func(string, uint) string,
) []fileChunk {
	mutex := sync.Mutex{}
	fileChunks := make([]fileChunk, 0)
	waitGroup := sync.WaitGroup{}

	appendFileChunk := func(chunk fileChunk) {
		mutex.Lock()
		defer mutex.Unlock()
		fileChunks = append(fileChunks, chunk)
	}

	for ix := range filePaths {
		waitGroup.Add(1)

		go func(fp string) {
			defer waitGroup.Done()

			if err := splitFn(fp, func(chunk uint, data []byte) error {
				outputFile := splitNameFn(fp, chunk)
				file, err := os.Create(outputFile)

				if err != nil {
					return err
				}

				defer file.Close()
				writer := bufio.NewWriter(file)
				_, err1 := writer.Write(data)
				return err1
			}); err != nil {
				panic(err)
			}

			newChunk := fileChunk{filePath: fp, chunks: mapOpCount}
			appendFileChunk(newChunk)
		}(filePaths[ix])
	}

	waitGroup.Wait()
	return fileChunks
}

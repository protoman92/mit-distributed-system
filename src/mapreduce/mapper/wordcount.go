package mapper

import (
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

const (
	// MapWordCountFn is the name for the MapWordCount function.
	MapWordCountFn MapFuncName = "MapWordCount"
)

// MapWordCount maps on word count.
func MapWordCount(key string, value [][]byte) []*mrutil.KeyValue {
	mutex := sync.RWMutex{}
	wordMap := make(map[string]int, 0)
	waitGroup := sync.WaitGroup{}

	updateWordMap := func(words []string) {
		for ix := range words {
			word := words[ix]
			mutex.Lock()
			wordMap[word] = wordMap[word] + 1
			mutex.Unlock()
		}
	}

	for ix := range value {
		waitGroup.Add(1)
		bytes := value[ix]

		go func(bytes []byte) {
			line := string(bytes)

			words := strings.FieldsFunc(line, func(r rune) bool {
				return !unicode.IsDigit(r) && !unicode.IsLetter(r)
			})

			updateWordMap(words)
			waitGroup.Done()
		}(bytes)
	}

	waitGroup.Wait()
	results := make([]*mrutil.KeyValue, 0)

	for key := range wordMap {
		kv := &mrutil.KeyValue{Key: key, Value: strconv.Itoa(wordMap[key])}
		results = append(results, kv)
	}

	return results
}

package mapper

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

const (
	// MapWordCountFn is the name for the MapWordCount function.
	MapWordCountFn MapFuncName = "MapWordCount"
)

// MapWordCount maps on word count.
func MapWordCount(key string, value []byte) []*mrutil.KeyValue {
	wordMap := make(map[string]int, 0)
	str := string(value)

	words := strings.FieldsFunc(str, func(r rune) bool {
		return !unicode.IsDigit(r) && !unicode.IsLetter(r)
	})

	for ix := range words {
		word := words[ix]
		wordMap[word] = wordMap[word] + 1
	}

	results := make([]*mrutil.KeyValue, 0)

	for key := range wordMap {
		kv := &mrutil.KeyValue{Key: key, Value: strconv.Itoa(wordMap[key])}
		results = append(results, kv)
	}

	return results
}

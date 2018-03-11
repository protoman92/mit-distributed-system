package mapper

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

// MapFunc represents a Map function.
type MapFunc func(string, [][]byte) []*mrutil.KeyValue

// MapFuncName represents a MapFunc name.
type MapFuncName string

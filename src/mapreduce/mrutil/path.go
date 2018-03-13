package mrutil

import (
	"fmt"
	"path"
)

// MapFileName creates a file name for a Map operation.
func MapFileName(fp string, mapNo uint) string {
	dir, file := path.Split(fp)
	newName := fmt.Sprintf("M%d-%s", mapNo, file)
	return path.Join(dir, newName)
}

// ReduceFileName creates a file name for a Reduce operation.
func ReduceFileName(fp string, mapNo uint, reduceNo uint) string {
	mapName := MapFileName(fp, mapNo)
	dir, file := path.Split(mapName)
	newName := fmt.Sprintf("R%d-%s", reduceNo, file)
	return path.Join(dir, newName)
}

// MergeFileName creates a file name for a Merge operation.
func MergeFileName(fp string, mapNo uint) string {
	dir, file := path.Split(fp)
	newName := fmt.Sprintf("result-%d-%s", mapNo, file)
	return path.Join(dir, newName)
}

// SplitFileName creates a file name for a Split operation.
func SplitFileName(fp string, chunk uint) string {
	dir, name := path.Split(fp)
	splitFP := fmt.Sprintf("%d-%s", chunk, name)
	return path.Join(dir, splitFP)
}

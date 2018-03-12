package mrutil

import (
	"fmt"
	"path"
)

// MapFileName creates a file name for a Map operation.
func MapFileName(fp string, mapNo uint) string {
	dir, file := path.Split(fp)
	newName := fmt.Sprintf("%d-%s", mapNo, file)
	return path.Join(dir, newName)
}

// ReduceFileName creates a file name for a Reduce operation.
func ReduceFileName(fp string, mapNo uint, reduceNo uint) string {
	mapName := MapFileName(fp, mapNo)
	dir, file := path.Split(mapName)
	newName := fmt.Sprintf("%d-%s", reduceNo, file)
	return path.Join(dir, newName)
}

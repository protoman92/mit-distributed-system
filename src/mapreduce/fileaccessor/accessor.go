package fileaccessor

// FileAccessor accesses files using some URI. This is useful for accessing
// remote files during a Reduce process, but if we are running the entire
// MapReduce in a single machine, a local file accessor would suffice.
type FileAccessor interface {
	AccessFile(uri string, fn func([]byte) error) error
}

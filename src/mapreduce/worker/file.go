package worker

import (
	"io/ioutil"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/fileaccess"
)

// ServeFile serves a file at some path.
func (d *WkDelegate) ServeFile(request fileaccess.AccessArgs, reply *fileaccess.AccessReply) error {
	resultCh := make(chan error, 0)

	d.accessFileCh <- fileaccess.AccessCallResult{
		ErrCh:   resultCh,
		Request: request,
		Reply:   reply,
	}

	return <-resultCh
}

// Other workers will try to access local files in this worker's system, esp.
// during reduce jobs.
func (w *worker) loopFileAccess() {
	for {
		select {
		case <-w.shutdownCh:
			return

		case result := <-w.Delegate.accessFileCh:
			go func() {
				file, err := os.Open(result.Request.File)

				if err != nil {
					result.ErrCh <- err
					return
				}

				defer file.Close()
				data, err := ioutil.ReadAll(file)

				if err != nil {
					result.ErrCh <- err
					return
				}

				result.Reply.Data = data
				result.ErrCh <- nil
			}()
		}
	}
}

package executor

import mrutil "github.com/protoman92/mit-distributed-system/src/mapreduce/util"

func (e *executor) String() string {
	return "Executor"
}

func (e *executor) DoneChannel() <-chan interface{} {
	return e.doneCh
}

func (e *executor) ErrorChannel() <-chan error {
	return e.errCh
}

func (e *executor) InputReceiptChannel() chan<- *mrutil.KeyValueChunk {
	return e.inputCh
}

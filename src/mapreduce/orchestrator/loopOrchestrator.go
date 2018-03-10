package orchestrator

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

func (o *orchestrator) loopReadInput() {
	readInputCh := o.InputReader.ReadInputChannel()
	resetCh := make(chan interface{}, 1)
	var excInputCh chan<- *mrutil.DataChunk
	var keyValue *mrutil.DataChunk

	for {
		select {
		case kv, ok := <-readInputCh:
			if !ok {
				o.LogMan.Printf("Finished reading input\n")
				return
			}

			keyValue = kv
			excInputCh = o.Executor.InputReceiptChannel()

		case excInputCh <- keyValue:
			excInputCh = nil
			keyValue = nil
			resetCh <- true

		case <-resetCh:
			readInputCh = o.InputReader.ReadInputChannel()
		}
	}
}

func (o *orchestrator) loopError() {
	for {
		select {
		case err := <-o.Executor.ErrorChannel():
			o.errCh <- err

		case err := <-o.InputReader.ErrorChannel():
			o.errCh <- err
		}
	}
}

package orchestrator

import (
	"fmt"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

func (o *orchestrator) loopReadInput() {
	for {
		select {
		case kvPipe := <-o.InputReader.ReadInputChannel():
			go func() {
				resetCh := make(chan interface{}, 1)
				rwInputCh := kvPipe.ValueCh
				var splitterInputCh chan<- *util.KeyValueSize
				var keyValue *util.KeyValueSize

				for {
					select {
					case input, ok := <-rwInputCh:
						if !ok {
							return
						}

						rwInputCh = nil
						splitterInputCh = o.Splitter.InputReceiptChannel()

						keyValue = &util.KeyValueSize{
							KeyValue:  &util.KeyValue{Key: kvPipe.Key, Value: input},
							TotalSize: kvPipe.TotalSize,
						}

					case splitterInputCh <- keyValue:
						splitterInputCh = nil
						keyValue = nil
						resetCh <- true

					case <-resetCh:
						rwInputCh = kvPipe.ValueCh
					}
				}
			}()
		}
	}
}

func (o *orchestrator) loopReceiveSplitResult() {
	splitResultCh := o.Splitter.SplitResultChannel()
	resetCh := make(chan interface{}, 1)
	splitToken := o.Splitter.SeparatorToken()
	var excInputCh chan<- *util.KeyValue
	var keyValue *util.KeyValue

	for {
		select {
		case kvPipe := <-splitResultCh:
			splitResultCh = nil
			data := make([]byte, 0)

			// We must be sure to close the split channels.
			for split := range kvPipe.ValueCh {
				data = append(data, split...)

				// We need to add back the separator.
				data = append(data, splitToken)
			}

			fmt.Printf("Received a chunk of length %d\n", len(data))
			excInputCh = o.Executor.InputReceiptChannel()
			keyValue = &util.KeyValue{Key: kvPipe.Key, Value: data}

		case excInputCh <- keyValue:
			excInputCh = nil
			keyValue = nil
			resetCh <- true

		case <-resetCh:
			splitResultCh = o.Splitter.SplitResultChannel()
		}
	}
}

func (o *orchestrator) loopDoneInput() {
	rwDoneInputCh := o.InputReader.DoneInputChannel()
	resetCh := make(chan interface{}, 1)
	var splitDoneReceiptCh chan<- interface{}

	for {
		select {
		case <-rwDoneInputCh:
			rwDoneInputCh = nil
			splitDoneReceiptCh = o.Splitter.DoneReceiptChannel()

		case splitDoneReceiptCh <- true:
			splitDoneReceiptCh = nil
			resetCh <- true

		case <-resetCh:
			rwDoneInputCh = o.InputReader.DoneInputChannel()
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

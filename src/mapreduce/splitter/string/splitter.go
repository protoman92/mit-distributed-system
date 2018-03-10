package string

import (
	"github.com/protoman92/mit-distributed-system/src/util"

	sp "github.com/protoman92/mit-distributed-system/src/mapreduce/splitter"
	mrutil "github.com/protoman92/mit-distributed-system/src/mapreduce/util"
)

// Params represents the required parameters to build a StringSplitter.
type Params struct {
	ChunkCount uint
	LogMan     util.LogMan
	SplitToken byte
}

func checkParams(params *Params) *Params {
	if params.ChunkCount == 0 || params.SplitToken == 0 {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{})
	}

	return params
}

type stringSplitter struct {
	*Params
	doneReceiptCh chan interface{}
	inputCh       chan *mrutil.KeyValueSize
	splitCh       chan *mrutil.KeyValuePipe
}

func (ss *stringSplitter) DoneReceiptChannel() chan<- interface{} {
	return ss.doneReceiptCh
}

func (ss *stringSplitter) InputReceiptChannel() chan<- *mrutil.KeyValueSize {
	return ss.inputCh
}

func (ss *stringSplitter) SeparatorToken() byte {
	return ss.SplitToken
}

func (ss *stringSplitter) SplitResultChannel() <-chan *mrutil.KeyValuePipe {
	return ss.splitCh
}

// NewStringSplitter returns a new StringSplitter.
func NewStringSplitter(params Params) sp.Splitter {
	checked := checkParams(&params)

	splitter := &stringSplitter{
		Params:        checked,
		doneReceiptCh: make(chan interface{}, 0),
		inputCh:       make(chan *mrutil.KeyValueSize, 0),
		splitCh:       make(chan *mrutil.KeyValuePipe, 0),
	}

	go splitter.loopWork()
	return splitter
}

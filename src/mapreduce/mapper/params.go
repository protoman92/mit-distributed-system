package mapper

import (
	"github.com/protoman92/gocompose/pkg"
)

// Params represents the required parameters to build a Mapper.
type Params struct {
	RetryWithDelay compose.ErrorTransformFunc
}

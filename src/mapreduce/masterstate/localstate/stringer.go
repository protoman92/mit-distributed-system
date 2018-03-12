package localstate

import (
	"fmt"
)

func (s *localState) String() string {
	return fmt.Sprint(s.jobs)
}

package master

import (
	"fmt"
	"strings"
)

func (m *master) String() string {
	return "Master"
}

func (r JobRequest) String() string {
	return fmt.Sprintf("Job request:\n %s", strings.Join(r.FilePaths, "\n"))
}

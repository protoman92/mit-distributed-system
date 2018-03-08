package main

import (
	"fmt"
	"os"
	"time"

	"github.com/protoman92/mit-distributed-system-6.824/src/viewservice"
)

//
// see directions in pbc.go
//

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: viewd port\n")
		os.Exit(1)
	}

	viewservice.StartServer(os.Args[1])

	for {
		time.Sleep(100 * time.Second)
	}
}

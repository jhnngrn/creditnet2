package main

import (
	"log"

	"creditnet"
	"creditnet/modules/loops"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	srv.Register("loops", loops.NewLoops)
	creditnet.RunAndWait(srv)
}

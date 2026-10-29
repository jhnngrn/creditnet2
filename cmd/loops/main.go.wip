package main

import (
	"log"

	"creditnet"
	"creditnet/modules/loops"
)

func main() {
	srv, err := creditnet.Boot("creditnet")
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, []string{"loops"})
	creditnet.Register(srv, "loops", loops.NewLoops)
	creditnet.RunAndWait(srv)
}

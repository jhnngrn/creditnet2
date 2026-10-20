package main

import (
	"log"

	"creditnet"
	"creditnet/modules/barter"
	"creditnet/modules/loops"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, []string{"barter", "loops"})
	creditnet.Register(srv, "barter", barter.NewBarter)
	creditnet.Register(srv, "loops", loops.NewLoops)
	creditnet.RunAndWait(srv)
}

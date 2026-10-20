package main

import (
	"log"

	"creditnet"
	"creditnet/modules/barter"
	"creditnet/modules/triad"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, []string{"barter", "triad"})
	creditnet.Register(srv, "barter", barter.NewBarter)
	creditnet.Register(srv, "triad", triad.NewTriad)
	creditnet.RunAndWait(srv)
}

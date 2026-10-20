package main

import (
	"log"

	"creditnet"
	"creditnet/modules/barter"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, []string{"barter"})
	creditnet.Register(srv, "barter", barter.NewBarter)
	creditnet.RunAndWait(srv)
}

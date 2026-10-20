package main

import (
	"log"

	"creditnet"
	"creditnet/modules/barter"
	"creditnet/modules/demurrage"
	"creditnet/modules/loops"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	// creditnet → demurrage → {barter, loops}
	creditnet.RegisterNode(srv, []string{"demurrage"})
	creditnet.Register(srv, "demurrage", demurrage.NewDemurrage)
	srv.DeclareChildren("demurrage", []string{"barter", "loops"})
	creditnet.Register(srv, "barter", barter.NewBarter)
	creditnet.Register(srv, "loops", loops.NewLoops)
	creditnet.RunAndWait(srv)
}

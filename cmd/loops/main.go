package main

import (
	"log"

	"creditnet"
	"creditnet/modules/demurrage"
	"creditnet/modules/loops"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	// creditnet → demurrage → loops
	creditnet.RegisterNode(srv, []string{"demurrage"})
	creditnet.Register(srv, "demurrage", demurrage.NewDemurrage)
	srv.DeclareChildren("demurrage", []string{"loops"})
	creditnet.Register(srv, "loops", loops.NewLoops)
	creditnet.RunAndWait(srv)
}

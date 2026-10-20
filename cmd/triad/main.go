package main

import (
	"log"

	"creditnet"
	"creditnet/modules/demurrage"
	"creditnet/modules/triad"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	// creditnet → demurrage → triad
	creditnet.RegisterNode(srv, []string{"demurrage"})
	creditnet.Register(srv, "demurrage", demurrage.NewDemurrage)
	srv.DeclareChildren("demurrage", []string{"triad"})
	creditnet.Register(srv, "triad", triad.NewTriad)
	creditnet.RunAndWait(srv)
}

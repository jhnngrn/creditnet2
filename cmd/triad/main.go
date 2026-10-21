package main

import (
	"log"

	"creditnet"
	"creditnet/modules/triad"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, []string{"triad"})
	creditnet.Register(srv, "triad", triad.NewTriad)

	if err := srv.Init(); err != nil {
		log.Fatal(err)
	}

creditnet.RunAndServe(srv)
}

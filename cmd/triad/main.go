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
	srv.Register("triad", triad.NewTriad)
	creditnet.RunAndWait(srv)
}

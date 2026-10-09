package main

import (
	"log"

	"creditnet/core"
)

func main() {
	srv, err := core.Boot()
	if err != nil {
		log.Fatal(err)
	}
	core.RunAndWait(srv)
}

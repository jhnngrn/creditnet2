package main

import (
	"log"

	"creditnet/base"
	"creditnet/core"
)

func main() {
	srv, err := core.Boot()
	if err != nil {
		log.Fatal(err)
	}
	srv.Register("base", base.NewHandler)
	core.RunAndWait(srv)
}

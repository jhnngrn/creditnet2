package main

import (
	"log"

	"creditnet2/base"
	"creditnet2/core"
)

func main() {
	srv, err := core.Boot()
	if err != nil {
		log.Fatal(err)
	}
	srv.Register("base", base.NewHandler)
	core.RunAndWait(srv)
}

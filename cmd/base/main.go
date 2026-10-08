package main

import (
	"log"
	"creditnet2/core"
)

func main() {
	srv, err := core.Boot()
	if err != nil { log.Fatal(err) }
	core.RunAndWait(srv)
}

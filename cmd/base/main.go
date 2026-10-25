package main

import (
	"log"

	"creditnet"
)

func main() {
	srv, err := creditnet.Boot("creditnet")
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, nil)
	creditnet.RunAndWait(srv)
}

package main

import (
	"log"

	"creditnet"
)

func main() {
	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RunAndWait(srv)
}

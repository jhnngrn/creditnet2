package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"creditnet"
	"creditnet/modules/triad"
)

func main() {
	webDir := flag.String("web", "", "Directory with static web files (optional)")

	srv, err := creditnet.Boot()
	if err != nil {
		log.Fatal(err)
	}
	creditnet.RegisterNode(srv, []string{"triad"})
	creditnet.Register(srv, "triad", triad.NewTriad)

	if err := srv.Init(); err != nil {
		log.Fatal(err)
	}

	// Serve static files from web directory if provided.
	if *webDir != "" {
		if info, err := os.Stat(*webDir); err != nil || !info.IsDir() {
			log.Fatalf("web directory %q not found", *webDir)
		}
		srv.Mux().Handle("GET /", http.FileServer(http.Dir(*webDir)))
		log.Printf("serving web UI from %s", *webDir)
	}

	creditnet.RunAndServe(srv)
}

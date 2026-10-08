package core

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

// Run parses flags, initializes the server, and blocks until signal.
// Call Register on the returned server before calling Run.
func Boot() (*Server, error) {
	domain := flag.String("domain", "", "Server domain (required)")
	dataDir := flag.String("data", "/var/lib/creditnet", "Data directory")
	port := flag.Int("port", 3000, "HTTP port")
	scheme := flag.String("scheme", "https", "Peer scheme")
	flag.Parse()

	if *domain == "" { *domain = os.Getenv("DOMAIN") }
	if *domain == "" { return nil, fmt.Errorf("domain is required (-domain or DOMAIN env)") }

	srv, err := NewServer(Config{
		Domain: *domain, Listen: fmt.Sprintf(":%d", *port),
		DataDir: *dataDir, PeerScheme: *scheme,
		AdminUser: "admin", AdminPass: "admin", Verbose: true,
	})
	return srv, err
}

func RunAndWait(srv *Server) {
	if err := srv.Init(); err != nil { log.Fatal(err) }

	ln, err := net.Listen("tcp", srv.cfg.Listen)
	if err != nil { log.Fatal(err) }
	log.Printf("creditnet on %s (%s)", ln.Addr(), srv.cfg.Domain)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { sig := make(chan os.Signal, 1); signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM); <-sig; cancel() }()
	if err := srv.Serve(ctx, ln); err != nil && ctx.Err() == nil { log.Fatal(err) }
}

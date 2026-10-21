package mimesis

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

// Boot parses flags and creates a Server. Call Register on the returned
// server to add module factories, then RunAndWait.
func Boot() (*Server, error) {
	domain := flag.String("domain", "", "Server domain (required)")
	dataDir := flag.String("data", "/var/lib/mimesis", "Data directory")
	port := flag.Int("port", 3000, "HTTP port")
	scheme := flag.String("scheme", "https", "Peer scheme")
	flag.Parse()

	if *domain == "" {
		*domain = os.Getenv("DOMAIN")
	}
	if *domain == "" {
		return nil, fmt.Errorf("domain is required (-domain or DOMAIN env)")
	}

	srv, err := NewServer(Config{
		Domain:     *domain,
		Listen:     fmt.Sprintf(":%d", *port),
		DataDir:    *dataDir,
		PeerScheme: *scheme,
		AdminUser:  "admin",
		AdminPass:  "admin",
		Verbose:    true,
	})
	return srv, err
}

// RunAndWait initializes the server and blocks until interrupted.
func RunAndWait(srv *Server) {
	if err := srv.Init(); err != nil {
		log.Fatal(err)
	}
	RunAndServe(srv)
}

// RunAndServe listens and blocks until interrupted.
// Unlike RunAndWait, it assumes Init has already been called, so extra
// routes can be added to the mux between Init and RunAndServe.
func RunAndServe(srv *Server) {
	ln, err := net.Listen("tcp", srv.cfg.Listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("mimesis on %s (%s)", ln.Addr(), srv.cfg.Domain)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		cancel()
	}()
	if err := srv.Serve(ctx, ln); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

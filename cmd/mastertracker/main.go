package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/New-pro125/distributed-file-system/mastertracker"
)

func main() {
	port := flag.Int("port", 50051, "gRPC listen port for the Master Tracker")
	flag.Parse()

	mt := mastertracker.New()

	// Graceful shutdown on SIGINT / SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, shutting down…", sig)
		mt.Stop()
	}()

	// Try to find an available port starting from the specified port
	finalPort := *port
	maxAttempts := 100
	var lastErr error

	for i := 0; i < maxAttempts; i++ {
		// Try to listen on the port to check availability
		testAddr := fmt.Sprintf(":%d", finalPort)
		listener, err := net.Listen("tcp", testAddr)

		if err != nil {
			// Port is in use, try next one
			log.Printf("Port %d is in use, trying port %d...", finalPort, finalPort+1)
			finalPort++
			lastErr = err
			continue
		}

		// Port is available, close the test listener and start the server
		listener.Close()
		log.Printf("Master Tracker starting on port %d", finalPort)

		if err := mt.Start(finalPort); err != nil {
			log.Fatalf("MasterTracker failed: %v", err)
		}
		return
	}

	// If we exhausted all attempts
	log.Fatalf("Could not find available port after %d attempts. Last error: %v", maxAttempts, lastErr)
}

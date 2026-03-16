package main

import (
	"flag"
	"log"
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

	if err := mt.Start(*port); err != nil {
		log.Fatalf("MasterTracker failed: %v", err)
	}
}

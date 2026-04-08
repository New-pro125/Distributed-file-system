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
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

type Config struct {
	MASTER_PORT int `env:"MASTER_PORT,required"`
}

func main() {
	cfg := Config{MASTER_PORT: 50051}
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("Warning: .env not found, using defaults/flags: %v", err)
	}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("Invalid MASTER_PORT in environment: %v", err)
	}

	envPort := cfg.MASTER_PORT
	port := flag.Int("port", envPort, "gRPC listen port for the Master Tracker")
	flag.Parse()

	mt := mastertracker.New()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, shutting down…", sig)
		mt.Stop()
	}()

	finalPort := *port
	maxAttempts := 100
	var lastErr error

	for i := 0; i < maxAttempts; i++ {
		testAddr := fmt.Sprintf(":%d", finalPort)
		listener, err := net.Listen("tcp", testAddr)

		if err != nil {
			log.Printf("Port %d is in use, trying port %d...", finalPort, finalPort+1)
			finalPort++
			lastErr = err
			continue
		}

		listener.Close()
		log.Printf("Master Tracker starting on port %d", finalPort)

		if err := mt.Start(finalPort); err != nil {
			log.Fatalf("MasterTracker failed: %v", err)
		}
		return
	}

	log.Fatalf("Could not find available port after %d attempts. Last error: %v", maxAttempts, lastErr)
}

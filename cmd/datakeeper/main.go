package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/New-pro125/distributed-file-system/datakeeper"
)

func main() {
	// Command-line flags
	id := flag.String("id", "dk1", "Unique ID for this DataKeeper")
	host := flag.String("host", "localhost", "Host address for this DataKeeper")
	grpcPort := flag.Int("grpc", 50052, "gRPC port for receiving replication commands")
	tcpPort := flag.Int("tcp", 3001, "TCP port for file transfers")
	storageDir := flag.String("storage", "./data", "Directory to store files")
	masterAddr := flag.String("master", "localhost:50051", "Address of the Master Tracker")
	
	flag.Parse()

	// Convert storage directory to absolute path
	absStorageDir, err := filepath.Abs(*storageDir)
	if err != nil {
		log.Fatalf("Failed to get absolute path for storage directory: %v", err)
	}

	log.Printf("Starting DataKeeper with ID: %s", *id)
	log.Printf("Host: %s, gRPC Port: %d, TCP Port: %d", *host, *grpcPort, *tcpPort)
	log.Printf("Storage Directory: %s", absStorageDir)
	log.Printf("Master Tracker: %s", *masterAddr)

	// Create DataKeeper instance
	dk, err := datakeeper.New(*id, *host, int32(*grpcPort), int32(*tcpPort), absStorageDir, *masterAddr)
	if err != nil {
		log.Fatalf("Failed to create DataKeeper: %v", err)
	}

	// Start DataKeeper services
	if err := dk.Start(); err != nil {
		log.Fatalf("Failed to start DataKeeper: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	fmt.Println("DataKeeper is running. Press Ctrl+C to stop.")
	<-sigChan
	
	log.Println("Shutting down DataKeeper...")
	dk.Stop()
	log.Println("DataKeeper stopped")
}

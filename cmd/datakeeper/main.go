package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/New-pro125/distributed-file-system/datakeeper"
)

func main() {
	// Command-line flags
	id := flag.String("id", "", "Unique ID for this DataKeeper (required)")
	host := flag.String("host", "localhost", "Host address for this DataKeeper")
	grpcPort := flag.Int("grpc", 50052, "gRPC port for receiving replication commands")
	tcpPort := flag.Int("tcp", 3001, "TCP port for file transfers")
	storageDir := flag.String("storage", "", "Directory to store files (default: ~/Desktop/Datakeep{id})")
	masterAddr := flag.String("master", "localhost:50051", "Address of the Master Tracker")

	flag.Parse()

	// Validate required ID parameter
	if *id == "" {
		log.Fatal("Error: -id parameter is required (e.g., -id=dk1)")
	}

	// Set default storage directory if not provided
	if *storageDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("Failed to get user home directory: %v", err)
		}
		*storageDir = filepath.Join(homeDir, "Desktop", fmt.Sprintf("Datakeep%s", *id))
	}

	// Convert storage directory to absolute path
	absStorageDir, err := filepath.Abs(*storageDir)
	if err != nil {
		log.Fatalf("Failed to get absolute path for storage directory: %v", err)
	}

	log.Printf("Starting DataKeeper with ID: %s", *id)
	log.Printf("Host: %s", *host)
	log.Printf("Storage Directory: %s", absStorageDir)
	log.Printf("Master Tracker: %s", *masterAddr)

	// Try to find available ports starting from the specified ports
	finalGrpcPort := *grpcPort
	finalTcpPort := *tcpPort
	maxAttempts := 100

	for i := 0; i < maxAttempts; i++ {
		// Check if gRPC port is available
		grpcListener, grpcErr := net.Listen("tcp", fmt.Sprintf(":%d", finalGrpcPort))
		if grpcErr != nil {
			log.Printf("gRPC port %d is in use, trying next port...", finalGrpcPort)
			finalGrpcPort++
			finalTcpPort++
			continue
		}
		grpcListener.Close()

		// Check if TCP port is available
		tcpListener, tcpErr := net.Listen("tcp", fmt.Sprintf(":%d", finalTcpPort))
		if tcpErr != nil {
			log.Printf("TCP port %d is in use, trying next port...", finalTcpPort)
			finalGrpcPort++
			finalTcpPort++
			continue
		}
		tcpListener.Close()

		// Both ports are available
		log.Printf("Using gRPC Port: %d, TCP Port: %d", finalGrpcPort, finalTcpPort)
		break
	}

	// Create DataKeeper instance
	dk, err := datakeeper.New(*id, *host, int32(finalGrpcPort), int32(finalTcpPort), absStorageDir, *masterAddr)
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

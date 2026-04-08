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
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

type Config struct {
	DK_ID         string `env:"DK_ID,required"`
	DK_HOST       string `env:"DK_HOST,required"`
	MASTER_ADDR   string `env:"MASTER_ADDR,required"`
	DK_STORAGE_DIR string `env:"DK_STORAGE_DIR"`
	DK_GRPC_PORT  int    `env:"DK_GRPC_PORT" envDefault:"50052"`
	DK_TCP_PORT   int    `env:"DK_TCP_PORT" envDefault:"3001"`
}

func main() {
	cfg := Config{}

	if err := godotenv.Load(".env"); err != nil {
		log.Printf("Warning: .env not found, using system env/flags: %v", err)
	}
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("Invalid environment variables in .env: %v", err)
	}
	envID := cfg.DK_ID
	envHost := cfg.DK_HOST
	envMaster := cfg.MASTER_ADDR
	envStorage := cfg.DK_STORAGE_DIR
	envGrpcPort := cfg.DK_GRPC_PORT
	envTcpPort := cfg.DK_TCP_PORT

	id := flag.String("id", envID, "Unique ID for this DataKeeper (required)")
	host := flag.String("host", envHost, "Host address for this DataKeeper")
	grpcPort := flag.Int("grpc", envGrpcPort, "gRPC port for receiving replication commands")
	tcpPort := flag.Int("tcp", envTcpPort, "TCP port for file transfers")
	storageDir := flag.String("storage", envStorage, "Directory to store files (default: ~/Desktop/Datakeep{id})")
	masterAddr := flag.String("master", envMaster, "Address of the Master Tracker (host:port)")

	flag.Parse()

	if *id == "" {
		log.Fatal("Error: DataKeeper ID is required via -id or DK_ID in .env")
	}

	if *host == "" {
		log.Fatal("Error: DataKeeper host is required via -host or DK_HOST in .env")
	}
	if ip := net.ParseIP(*host); ip != nil {
		if ip.IsLoopback() || ip.IsUnspecified() {
			log.Fatalf("Error: DK_HOST must be a reachable LAN IP, got %q", *host)
		}
	} else if *host == "localhost" {
		log.Fatalf("Error: DK_HOST must be a reachable LAN IP or DNS name, got %q", *host)
	}

	if *masterAddr == "" {
		log.Fatal("Error: Master address is required via -master or MASTER_ADDR in .env")
	}

	if *storageDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("Failed to get user home directory: %v", err)
		}
		*storageDir = filepath.Join(homeDir, "Desktop", fmt.Sprintf("Datakeep%s", *id))
	}

	absStorageDir, err := filepath.Abs(*storageDir)
	if err != nil {
		log.Fatalf("Failed to get absolute path for storage directory: %v", err)
	}

	log.Printf("Starting DataKeeper with ID: %s", *id)
	log.Printf("Host: %s", *host)
	log.Printf("Storage Directory: %s", absStorageDir)
	log.Printf("Master Tracker: %s", *masterAddr)

	finalGrpcPort := *grpcPort
	finalTcpPort := *tcpPort
	maxAttempts := 100

	for i := 0; i < maxAttempts; i++ {
		grpcListener, grpcErr := net.Listen("tcp", fmt.Sprintf(":%d", finalGrpcPort))
		if grpcErr != nil {
			log.Printf("gRPC port %d is in use, trying next port...", finalGrpcPort)
			finalGrpcPort++
			finalTcpPort++
			continue
		}
		grpcListener.Close()

		tcpListener, tcpErr := net.Listen("tcp", fmt.Sprintf(":%d", finalTcpPort))
		if tcpErr != nil {
			log.Printf("TCP port %d is in use, trying next port...", finalTcpPort)
			finalGrpcPort++
			finalTcpPort++
			continue
		}
		tcpListener.Close()
		log.Printf("Using gRPC Port: %d, TCP Port: %d", finalGrpcPort, finalTcpPort)
		break
	}

	dk, err := datakeeper.New(*id, *host, int32(finalGrpcPort), int32(finalTcpPort), absStorageDir, *masterAddr)
	if err != nil {
		log.Fatalf("Failed to create DataKeeper: %v", err)
	}

	if err := dk.Start(); err != nil {
		log.Fatalf("Failed to start DataKeeper: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("DataKeeper is running. Press Ctrl+C to stop.")
	<-sigChan

	log.Println("Shutting down DataKeeper...")
	dk.Stop()
	log.Println("DataKeeper stopped")
}

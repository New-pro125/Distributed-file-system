package datakeeper

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/New-pro125/distributed-file-system/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer

	id       string
	host     string
	grpcPort int32
	tcpPort  int32

	storageDir string // directory where files are stored

	masterConn   *grpc.ClientConn
	masterClient pb.MasterTrackerClient

	heartbeatTicker *time.Ticker
	done            chan struct{}

	mu sync.Mutex
}

// New creates a new DataKeeper instance
func New(id, host string, grpcPort, tcpPort int32, storageDir, masterAddr string) (*DataKeeper, error) {
	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Connect to Master Tracker
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	dk := &DataKeeper{
		id:              id,
		host:            host,
		grpcPort:        grpcPort,
		tcpPort:         tcpPort,
		storageDir:      storageDir,
		masterConn:      conn,
		masterClient:    pb.NewMasterTrackerClient(conn),
		heartbeatTicker: time.NewTicker(1 * time.Second),
		done:            make(chan struct{}),
	}

	return dk, nil
}

// Start begins the DataKeeper services
func (dk *DataKeeper) Start() error {
	// Scan for existing files in storage and notify master
	go dk.scanAndNotifyExistingFiles()

	// Start gRPC server
	go dk.startGRPCServer()

	// Start TCP file server
	go dk.startTCPServer()

	// Start heartbeat loop
	go dk.heartbeatLoop()

	log.Printf("DataKeeper %s started on gRPC port %d, TCP port %d", dk.id, dk.grpcPort, dk.tcpPort)
	return nil
}

// Stop gracefully shuts down the DataKeeper
func (dk *DataKeeper) Stop() {
	close(dk.done)
	dk.heartbeatTicker.Stop()
	if dk.masterConn != nil {
		dk.masterConn.Close()
	}
}

// scanAndNotifyExistingFiles scans the storage directory for existing files
// and notifies the master tracker about them
func (dk *DataKeeper) scanAndNotifyExistingFiles() {
	// Wait a bit for the datakeeper to fully start and establish connection
	time.Sleep(2 * time.Second)

	log.Printf("Scanning storage directory for existing files: %s", dk.storageDir)

	// Read directory contents
	entries, err := os.ReadDir(dk.storageDir)
	if err != nil {
		log.Printf("Warning: Failed to scan storage directory: %v", err)
		return
	}

	filesFound := 0
	for _, entry := range entries {
		// Skip directories, only process files
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		filePath := filepath.Join(dk.storageDir, fileName)

		// Notify master about this existing file
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := dk.masterClient.NotifyUploadDone(ctx, &pb.NotifyUploadRequest{
			FileName: fileName,
			NodeId:   dk.id,
			FilePath: filePath,
		})
		cancel()

		if err != nil {
			log.Printf("Warning: Failed to notify master about existing file %s: %v", fileName, err)
		} else {
			log.Printf("Registered existing file with master: %s", fileName)
			filesFound++
		}

		// Small delay to avoid overwhelming the master
		time.Sleep(100 * time.Millisecond)
	}

	if filesFound > 0 {
		log.Printf("Successfully registered %d existing file(s) with master", filesFound)
	} else {
		log.Printf("No existing files found in storage directory")
	}
}

// startGRPCServer starts the gRPC server for receiving NotifyTransfer calls
func (dk *DataKeeper) startGRPCServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", dk.grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %d: %v", dk.grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDataKeeperServer(grpcServer, dk)

	log.Printf("DataKeeper gRPC server listening on port %d", dk.grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// startTCPServer starts the TCP server for receiving file uploads
func (dk *DataKeeper) startTCPServer() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", dk.tcpPort))
	if err != nil {
		log.Fatalf("Failed to listen on TCP port %d: %v", dk.tcpPort, err)
	}
	defer listener.Close()

	log.Printf("DataKeeper TCP server listening on port %d", dk.tcpPort)

	for {
		select {
		case <-dk.done:
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Failed to accept TCP connection: %v", err)
				continue
			}
			go dk.handleTCPConnection(conn)
		}
	}
}

// handleTCPConnection handles incoming TCP connections for both uploads and downloads
func (dk *DataKeeper) handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	// Read operation code (1 byte): 0x01 = upload, 0x02 = download
	opCode := make([]byte, 1)
	if _, err := io.ReadFull(conn, opCode); err != nil {
		log.Printf("Failed to read operation code: %v", err)
		return
	}

	switch opCode[0] {
	case 0x01: // Upload
		dk.handleUpload(conn)
	case 0x02: // Download
		dk.handleDownload(conn)
	default:
		log.Printf("Unknown operation code: 0x%02x", opCode[0])
	}
}

// handleUpload receives a file upload from a client or another DataKeeper
func (dk *DataKeeper) handleUpload(conn net.Conn) {
	// Receive file using wire format
	fileName, fileData, err := recvFile(conn)
	if err != nil {
		log.Printf("Failed to receive file: %v", err)
		return
	}

	// Save file to disk
	filePath := filepath.Join(dk.storageDir, fileName)
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		log.Printf("Failed to write file %s: %v", fileName, err)
		return
	}

	log.Printf("Successfully received and saved file: %s (%d bytes)", fileName, len(fileData))

	// Notify Master Tracker that upload is done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = dk.masterClient.NotifyUploadDone(ctx, &pb.NotifyUploadRequest{
		FileName: fileName,
		NodeId:   dk.id,
		FilePath: filePath,
	})
	if err != nil {
		log.Printf("Failed to notify master of upload completion: %v", err)
	}
}

// handleDownload sends a file to a downloading client
func (dk *DataKeeper) handleDownload(conn net.Conn) {
	// Read the requested filename
	var nameLen uint32
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		log.Printf("Failed to read filename length: %v", err)
		return
	}

	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		log.Printf("Failed to read filename: %v", err)
		return
	}
	fileName := string(nameBuf)

	// Read file from disk
	filePath := filepath.Join(dk.storageDir, fileName)
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", fileName, err)
		return
	}

	// Send file using wire format
	if err := sendFile(conn, fileName, fileData); err != nil {
		log.Printf("Failed to send file %s: %v", fileName, err)
		return
	}

	log.Printf("Successfully sent file: %s (%d bytes)", fileName, len(fileData))
}

// heartbeatLoop sends periodic heartbeats to the Master Tracker
func (dk *DataKeeper) heartbeatLoop() {
	for {
		select {
		case <-dk.done:
			return
		case <-dk.heartbeatTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := dk.masterClient.Heartbeat(ctx, &pb.HeartbeatRequest{
				NodeId: dk.id,
				Address: &pb.NodeAddress{
					Host:     dk.host,
					TcpPort:  dk.tcpPort,
					GrpcPort: dk.grpcPort,
				},
			})
			cancel()

			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			} else if !resp.Accepted {
				log.Printf("Heartbeat rejected by master")
			}
		}
	}
}

// NotifyTransfer implements the gRPC NotifyTransfer handler
// This is called by the Master Tracker to trigger a DK-to-DK file transfer
func (dk *DataKeeper) NotifyTransfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	log.Printf("Received transfer request for file %s from %s:%d to %s:%d",
		req.FileName, req.Src.Host, req.Src.TcpPort, req.Dst.Host, req.Dst.TcpPort)

	// If this node is the source, read the file and send it to destination
	if req.Src.Host == dk.host && req.Src.TcpPort == dk.tcpPort {
		return dk.handleSourceTransfer(req)
	}

	// If this node is the destination, receive the file from source
	if req.Dst.Host == dk.host && req.Dst.TcpPort == dk.tcpPort {
		return dk.handleDestinationTransfer(req)
	}

	return &pb.TransferResponse{
		Success: false,
		Message: "This node is neither source nor destination",
	}, nil
}

// handleSourceTransfer reads the file and sends it to the destination node
func (dk *DataKeeper) handleSourceTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error) {
	// Read the file from disk
	fileData, err := os.ReadFile(req.FilePath)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to read file: %v", err),
		}, nil
	}

	// Connect to destination node's TCP server
	dstAddr := fmt.Sprintf("%s:%d", req.Dst.Host, req.Dst.TcpPort)
	conn, err := net.DialTimeout("tcp", dstAddr, 10*time.Second)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to connect to destination: %v", err),
		}, nil
	}
	defer conn.Close()

	// Send operation code (0x01 = upload)
	if _, err := conn.Write([]byte{0x01}); err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send operation code: %v", err),
		}, nil
	}

	// Send file using wire format
	if err := sendFile(conn, req.FileName, fileData); err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send file: %v", err),
		}, nil
	}

	log.Printf("Successfully transferred file %s to %s", req.FileName, dstAddr)
	return &pb.TransferResponse{
		Success: true,
		Message: "File transferred successfully",
	}, nil
}

// handleDestinationTransfer receives the file from the source node
func (dk *DataKeeper) handleDestinationTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error) {
	// The file will be received via the TCP server's handleTCPConnection
	// This handler just acknowledges the notification
	log.Printf("Ready to receive file %s from %s:%d", req.FileName, req.Src.Host, req.Src.TcpPort)
	return &pb.TransferResponse{
		Success: true,
		Message: "Ready to receive file",
	}, nil
}

// TCP Wire Format Helper Functions

// sendFile sends a file over TCP using the agreed wire format
func sendFile(conn net.Conn, name string, data []byte) error {
	nameBytes := []byte(name)

	// Write name length (4 bytes, uint32, big-endian)
	if err := binary.Write(conn, binary.BigEndian, uint32(len(nameBytes))); err != nil {
		return fmt.Errorf("failed to write name length: %w", err)
	}

	// Write filename
	if _, err := conn.Write(nameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}

	// Write file size (8 bytes, uint64, big-endian)
	if err := binary.Write(conn, binary.BigEndian, uint64(len(data))); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	// Write file data
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write file data: %w", err)
	}

	return nil
}

// recvFile receives a file over TCP using the agreed wire format
func recvFile(conn net.Conn) (name string, data []byte, err error) {
	// Read name length
	var nameLen uint32
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		return "", nil, fmt.Errorf("failed to read name length: %w", err)
	}

	// Read filename
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return "", nil, fmt.Errorf("failed to read filename: %w", err)
	}

	// Read file size
	var fileSize uint64
	if err := binary.Read(conn, binary.BigEndian, &fileSize); err != nil {
		return "", nil, fmt.Errorf("failed to read file size: %w", err)
	}

	// Read file data
	data = make([]byte, fileSize)
	if _, err := io.ReadFull(conn, data); err != nil {
		return "", nil, fmt.Errorf("failed to read file data: %w", err)
	}

	return string(nameBuf), data, nil
}

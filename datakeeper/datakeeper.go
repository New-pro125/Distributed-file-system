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

	storageDir string

	masterConn   *grpc.ClientConn
	masterClient pb.MasterTrackerClient

	heartbeatTicker *time.Ticker
	done            chan struct{}
}

func New(id, host string, grpcPort, tcpPort int32, storageDir, masterAddr string) (*DataKeeper, error) {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}
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
func (dk *DataKeeper) Start() error {
	if _, err := dk.sendHeartbeat(4 * time.Second); err != nil {
		return fmt.Errorf("cannot reach master tracker at startup (%s): %w", dk.masterConn.Target(), err)
	}

	go dk.scanAndNotifyExistingFiles()
	go dk.startGRPCServer()
	go dk.startTCPServer()
	go dk.heartbeatLoop()

	log.Printf("DataKeeper %s started on gRPC port %d, TCP port %d", dk.id, dk.grpcPort, dk.tcpPort)
	return nil
}
func (dk *DataKeeper) Stop() {
	close(dk.done)
	dk.heartbeatTicker.Stop()
	if dk.masterConn != nil {
		dk.masterConn.Close()
	}
}
func (dk *DataKeeper) scanAndNotifyExistingFiles() {
	time.Sleep(2 * time.Second)

	log.Printf("Scanning storage directory for existing files: %s", dk.storageDir)
	entries, err := os.ReadDir(dk.storageDir)
	if err != nil {
		log.Printf("Warning: Failed to scan storage directory: %v", err)
		return
	}

	filesFound := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		filePath := filepath.Join(dk.storageDir, fileName)
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

		time.Sleep(100 * time.Millisecond)
	}

	if filesFound > 0 {
		log.Printf("Successfully registered %d existing file(s) with master", filesFound)
	} else {
		log.Printf("No existing files found in storage directory")
	}
}
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
func (dk *DataKeeper) handleTCPConnection(conn net.Conn) {
	defer conn.Close()
	opCode := make([]byte, 1)
	if _, err := io.ReadFull(conn, opCode); err != nil {
		log.Printf("Failed to read operation code: %v", err)
		return
	}

	switch opCode[0] {
	case 0x01:
		dk.handleUpload(conn)
	case 0x02:
		dk.handleDownload(conn)
	default:
		log.Printf("Unknown operation code: 0x%02x", opCode[0])
	}
}
func (dk *DataKeeper) handleUpload(conn net.Conn) {
	var nameLen uint32
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		log.Printf("Failed to read name length: %v", err)
		return
	}

	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		log.Printf("Failed to read filename: %v", err)
		return
	}
	fileName := string(nameBuf)

	var fileSize uint64
	if err := binary.Read(conn, binary.BigEndian, &fileSize); err != nil {
		log.Printf("Failed to read file size: %v", err)
		return
	}

	filePath := filepath.Join(dk.storageDir, fileName)
	f, err := os.Create(filePath)
	if err != nil {
		log.Printf("Failed to create file %s: %v", filePath, err)
		return
	}

	written, copyErr := io.CopyN(f, conn, int64(fileSize))
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(filePath)
		log.Printf("Failed to receive file data for %s: %v", fileName, copyErr)
		return
	}
	if closeErr != nil {
		_ = os.Remove(filePath)
		log.Printf("Failed to finalize file %s: %v", fileName, closeErr)
		return
	}

	log.Printf("Successfully received and saved file: %s (%d bytes)", fileName, written)
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
func (dk *DataKeeper) handleDownload(conn net.Conn) {
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
	filePath := filepath.Join(dk.storageDir, fileName)
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", fileName, err)
		return
	}
	if err := sendFile(conn, fileName, fileData); err != nil {
		log.Printf("Failed to send file %s: %v", fileName, err)
		return
	}

	log.Printf("Successfully sent file: %s (%d bytes)", fileName, len(fileData))
}
func (dk *DataKeeper) heartbeatLoop() {
	for {
		select {
		case <-dk.done:
			return
		case <-dk.heartbeatTicker.C:
			resp, err := dk.sendHeartbeat(2 * time.Second)
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			} else if !resp.Accepted {
				log.Printf("Heartbeat rejected by master")
			}
		}
	}
}

func (dk *DataKeeper) sendHeartbeat(timeout time.Duration) (*pb.HeartbeatResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return dk.masterClient.Heartbeat(ctx, &pb.HeartbeatRequest{
		NodeId: dk.id,
		Address: &pb.NodeAddress{
			Host:     dk.host,
			TcpPort:  dk.tcpPort,
			GrpcPort: dk.grpcPort,
		},
	})
}
func (dk *DataKeeper) NotifyTransfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	log.Printf("Received transfer request for file %s from %s:%d to %s:%d",
		req.FileName, req.Src.Host, req.Src.TcpPort, req.Dst.Host, req.Dst.TcpPort)
	if req.Src.Host == dk.host && req.Src.TcpPort == dk.tcpPort {
		return dk.handleSourceTransfer(req)
	}
	if req.Dst.Host == dk.host && req.Dst.TcpPort == dk.tcpPort {
		return dk.handleDestinationTransfer(req)
	}

	return &pb.TransferResponse{
		Success: false,
		Message: "This node is neither source nor destination",
	}, nil
}
func (dk *DataKeeper) handleSourceTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error) {
	fileData, err := os.ReadFile(req.FilePath)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to read file: %v", err),
		}, nil
	}
	dstAddr := fmt.Sprintf("%s:%d", req.Dst.Host, req.Dst.TcpPort)
	conn, err := net.DialTimeout("tcp", dstAddr, 10*time.Second)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to connect to destination: %v", err),
		}, nil
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{0x01}); err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send operation code: %v", err),
		}, nil
	}
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
func (dk *DataKeeper) handleDestinationTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error) {
	log.Printf("Pulling file %s from source %s:%d", req.FileName, req.Src.Host, req.Src.TcpPort)
	srcAddr := fmt.Sprintf("%s:%d", req.Src.Host, req.Src.TcpPort)
	conn, err := net.DialTimeout("tcp", srcAddr, 10*time.Second)
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to connect to source: %v", err)}, nil
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{0x02}); err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to send operation code: %v", err)}, nil
	}

	if err := binary.Write(conn, binary.BigEndian, uint32(len(req.FileName))); err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to send filename length: %v", err)}, nil
	}
	if _, err := conn.Write([]byte(req.FileName)); err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to send filename: %v", err)}, nil
	}

	fileName, fileData, err := recvFile(conn)
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to receive file data: %v", err)}, nil
	}

	filePath := filepath.Join(dk.storageDir, fileName)
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to write to disk: %v", err)}, nil
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dk.masterClient.NotifyUploadDone(ctx, &pb.NotifyUploadRequest{
			FileName: fileName,
			NodeId:   dk.id,
			FilePath: filePath,
		})
	}()

	log.Printf("Successfully replicated file %s locally", fileName)
	return &pb.TransferResponse{Success: true, Message: "Replication pulled successfully"}, nil
}
func sendFile(conn net.Conn, name string, data []byte) error {
	nameBytes := []byte(name)
	if err := binary.Write(conn, binary.BigEndian, uint32(len(nameBytes))); err != nil {
		return fmt.Errorf("failed to write name length: %w", err)
	}
	if _, err := conn.Write(nameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}
	if err := binary.Write(conn, binary.BigEndian, uint64(len(data))); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write file data: %w", err)
	}

	return nil
}
func recvFile(conn net.Conn) (name string, data []byte, err error) {
	var nameLen uint32
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		return "", nil, fmt.Errorf("failed to read name length: %w", err)
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return "", nil, fmt.Errorf("failed to read filename: %w", err)
	}
	var fileSize uint64
	if err := binary.Read(conn, binary.BigEndian, &fileSize); err != nil {
		return "", nil, fmt.Errorf("failed to read file size: %w", err)
	}
	data = make([]byte, fileSize)
	if _, err := io.ReadFull(conn, data); err != nil {
		return "", nil, fmt.Errorf("failed to read file data: %w", err)
	}

	return string(nameBuf), data, nil
}

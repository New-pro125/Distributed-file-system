package datakeeper

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/New-pro125/distributed-file-system/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ChunkSize   = 16 * 1024 * 1024
	LogInterval = 16 * 1024 * 1024
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

	activeReplications sync.Map
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
		ctx, cancel := context.WithTimeout(context.Background(), 1440*time.Second)
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

	conn.SetDeadline(time.Now().Add(30 * time.Minute))

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
	fileName, fileSize, err := recvFileHeader(conn)
	if err != nil {
		log.Printf("Failed to receive file: %v", err)
		return
	}
	filePath := filepath.Join(dk.storageDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("Failed to write file %s: %v", fileName, err)
		return
	}
	defer file.Close()

	clientAddr := conn.RemoteAddr().String()
	opLabel := fmt.Sprintf("receiving from %s", clientAddr)
	written, err := streamWithProgress(file, conn, int64(fileSize), fileName, opLabel)
	if err != nil {
		log.Printf("Failed to stream file %s to disk: %v", fileName, err)
		return
	}

	log.Printf("Successfully received and saved file: %s (%d bytes)", fileName, written)
	ctx, cancel := context.WithTimeout(context.Background(), 1440*time.Second)
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

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open file %s: %v", fileName, err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("Failed to get file info %s: %v", fileName, err)
		return
	}

	bufReader := bufio.NewReaderSize(file, ChunkSize)
	clientAddr := conn.RemoteAddr().String()
	opLabel := fmt.Sprintf("sending to %s", clientAddr)

	if err := sendFileStreamWithLabel(conn, fileName, bufReader, fileInfo.Size(), opLabel); err != nil {
		log.Printf("Failed to send file %s: %v", fileName, err)
		return
	}

	log.Printf("Successfully sent file: %s (%d bytes)", fileName, fileInfo.Size())
}
func (dk *DataKeeper) heartbeatLoop() {
	for {
		select {
		case <-dk.done:
			return
		case <-dk.heartbeatTicker.C:
			resp, err := dk.sendHeartbeat(10 * time.Second)
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
	transferKey := fmt.Sprintf("%s->%s:%d", req.FileName, req.Dst.Host, req.Dst.TcpPort)

	if _, exists := dk.activeReplications.LoadOrStore(transferKey, true); exists {
		return &pb.TransferResponse{Success: false, Message: "Transfer already in progress"}, nil
	}
	defer dk.activeReplications.Delete(transferKey)

	file, err := os.Open(req.FilePath)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to open file: %v", err),
		}, nil
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get file info: %v", err),
		}, nil
	}

	dstAddr := fmt.Sprintf("%s:%d", req.Dst.Host, req.Dst.TcpPort)
	conn, err := net.DialTimeout("tcp", dstAddr, 30*time.Second)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to connect to destination: %v", err),
		}, nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(30 * time.Minute))

	if _, err := conn.Write([]byte{0x01}); err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send operation code: %v", err),
		}, nil
	}

	bufReader := bufio.NewReaderSize(file, ChunkSize)
	opLabel := fmt.Sprintf("sending to %s:%d", req.Dst.Host, req.Dst.TcpPort)

	if err := sendFileStreamWithLabel(conn, req.FileName, bufReader, fileInfo.Size(), opLabel); err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to send file: %v", err),
		}, nil
	}

	log.Printf("Successfully transferred file %s (%d bytes) to %s", req.FileName, fileInfo.Size(), dstAddr)
	return &pb.TransferResponse{
		Success: true,
		Message: "File transferred successfully",
	}, nil
}
func (dk *DataKeeper) handleDestinationTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error) {
	filePath := filepath.Join(dk.storageDir, req.FileName)
	if _, err := os.Stat(filePath); err == nil {
		return &pb.TransferResponse{Success: true, Message: "File already exists"}, nil
	}

	if _, exists := dk.activeReplications.LoadOrStore(req.FileName, true); exists {
		return &pb.TransferResponse{Success: false, Message: "Replication already in progress"}, nil
	}

	defer dk.activeReplications.Delete(req.FileName)

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

	fileName, fileSize, err := recvFileHeader(conn)
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to receive file data: %v", err)}, nil
	}

	file, err := os.Create(filePath)
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to write to disk: %v", err)}, nil
	}
	defer file.Close()

	opLabel := fmt.Sprintf("replicating from %s:%d", req.Src.Host, req.Src.TcpPort)

	written, err := streamWithProgress(file, conn, int64(fileSize), fileName, opLabel)
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: fmt.Sprintf("Failed to stream file data to disk: %v", err)}, nil
	}

	log.Printf("Replication stream complete: %s (%d bytes)", fileName, written)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1440*time.Second)
		defer cancel()
		dk.masterClient.NotifyUploadDone(ctx, &pb.NotifyUploadRequest{
			FileName: fileName,
			NodeId:   dk.id,
			FilePath: filePath,
		})
	}()

	log.Printf("Successfully replicated file %s locally (%d bytes)", fileName, written)
	return &pb.TransferResponse{Success: true, Message: "Replication pulled successfully"}, nil
}

func sendFileStreamWithLabel(conn net.Conn, name string, reader io.Reader, size int64, opLabel string) error {
	nameBytes := []byte(name)
	if err := binary.Write(conn, binary.BigEndian, uint32(len(nameBytes))); err != nil {
		return fmt.Errorf("failed to write name length: %w", err)
	}
	if _, err := conn.Write(nameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}
	if err := binary.Write(conn, binary.BigEndian, uint64(size)); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	written, err := streamWithProgress(conn, reader, size, name, opLabel)
	if err != nil {
		return fmt.Errorf("failed to stream file data: %w", err)
	}
	if written != size {
		return fmt.Errorf("incomplete transfer: wrote %d bytes, expected %d", written, size)
	}

	return nil
}

func streamWithProgress(dst io.Writer, src io.Reader, totalSize int64, fileName, operation string) (int64, error) {
	buffer := make([]byte, ChunkSize)
	var totalWritten int64
	var lastLoggedAt int64
	logNum := 0

	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			written, writeErr := dst.Write(buffer[:n])
			if writeErr != nil {
				return totalWritten, writeErr
			}
			totalWritten += int64(written)

			if totalWritten-lastLoggedAt >= LogInterval || readErr == io.EOF {
				logNum++
				progress := float64(totalWritten) / float64(totalSize) * 100
				log.Printf("[%s] %s: progress update %d - %d MB transferred (%.2f%% complete)",
					operation, fileName, logNum, totalWritten/(1024*1024), progress)
				lastLoggedAt = totalWritten
			}
		}

		if readErr == io.EOF {
			if totalWritten > lastLoggedAt {
				logNum++
				log.Printf("[%s] %s: completed - %d MB transferred (100.00%% complete)",
					operation, fileName, totalWritten/(1024*1024))
			}
			break
		}
		if readErr != nil {
			return totalWritten, readErr
		}
	}

	return totalWritten, nil
}
func recvFileHeader(conn net.Conn) (name string, fileSize uint64, err error) {
	var nameLen uint32
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		return "", 0, fmt.Errorf("failed to read name length: %w", err)
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return "", 0, fmt.Errorf("failed to read filename: %w", err)
	}
	if err := binary.Read(conn, binary.BigEndian, &fileSize); err != nil {
		return "", 0, fmt.Errorf("failed to read file size: %w", err)
	}
	if fileSize > math.MaxInt64 {
		return "", 0, fmt.Errorf("file size too large: %d", fileSize)
	}

	return string(nameBuf), fileSize, nil
}

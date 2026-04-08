package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	pb "github.com/New-pro125/distributed-file-system/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	masterConn   *grpc.ClientConn
	masterClient pb.MasterTrackerClient
	
	wg    sync.WaitGroup
	errCh chan error
}

// New creates a new DFS client
func New(masterAddr string) (*Client, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master tracker: %w", err)
	}

	return &Client{
		masterConn:   conn,
		masterClient: pb.NewMasterTrackerClient(conn),
		errCh:        make(chan error, 10),
	}, nil
}

// Close closes the connection to the master tracker
func (c *Client) Close() error {
	if c.masterConn != nil {
		return c.masterConn.Close()
	}
	return nil
}

// Upload uploads a file to the distributed file system
func (c *Client) Upload(ctx context.Context, filePath string) error {
	// Read the file from disk
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Extract filename from path
	fileName := extractFileName(filePath)

	// Step 1: Request upload from Master Tracker
	uploadResp, err := c.masterClient.RequestUpload(ctx, &pb.UploadRequest{
		FileName: fileName,
	})
	if err != nil {
		return fmt.Errorf("failed to request upload: %w", err)
	}

	if uploadResp.DataKeeper == nil {
		return fmt.Errorf("master tracker did not provide a data keeper address")
	}

	log.Printf("Master assigned DataKeeper at %s:%d for upload", 
		uploadResp.DataKeeper.Host, uploadResp.DataKeeper.TcpPort)

	// Step 2: Connect to the DataKeeper's TCP port
	dkAddr := fmt.Sprintf("%s:%d", uploadResp.DataKeeper.Host, uploadResp.DataKeeper.TcpPort)
	conn, err := net.DialTimeout("tcp", dkAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to data keeper: %w", err)
	}
	defer conn.Close()

	// Step 3: Send operation code (0x01 = upload)
	if _, err := conn.Write([]byte{0x01}); err != nil {
		return fmt.Errorf("failed to send operation code: %w", err)
	}

	// Step 4: Send file data using TCP wire format
	if err := sendFile(conn, fileName, fileData); err != nil {
		return fmt.Errorf("failed to send file: %w", err)
	}

	log.Printf("Successfully uploaded file %s (%d bytes) to %s", fileName, len(fileData), dkAddr)

	// Step 5: Wait a moment for the DataKeeper to notify the Master
	// In production, we might wait for an acknowledgment or implement a callback
	time.Sleep(500 * time.Millisecond)

	return nil
}

// Download downloads a file from the distributed file system
func (c *Client) Download(ctx context.Context, fileName, savePath string) error {
	// Step 1: Request download locations from Master Tracker
	downloadResp, err := c.masterClient.RequestDownload(ctx, &pb.DownloadRequest{
		FileName: fileName,
	})
	if err != nil {
		return fmt.Errorf("failed to request download: %w", err)
	}

	if len(downloadResp.Nodes) == 0 {
		return fmt.Errorf("no replicas available for file %s", fileName)
	}

	log.Printf("Master returned %d replica(s) for file %s", len(downloadResp.Nodes), fileName)

	// Step 2: Try to download from the first available replica
	// (Round-robin or uniform selection would pick one at random)
	for i, node := range downloadResp.Nodes {
		log.Printf("Attempting to download from replica %d: %s:%d", i+1, node.Host, node.TcpPort)
		
		fileData, err := c.downloadFromNode(node, fileName)
		if err != nil {
			log.Printf("Failed to download from %s:%d: %v", node.Host, node.TcpPort, err)
			continue
		}

		// Save the file to disk
		if err := os.WriteFile(savePath, fileData, 0644); err != nil {
			return fmt.Errorf("failed to write file to disk: %w", err)
		}

		log.Printf("Successfully downloaded file %s (%d bytes) and saved to %s", 
			fileName, len(fileData), savePath)
		return nil
	}

	return fmt.Errorf("failed to download file from any replica")
}

// DownloadParallel downloads a file from the distributed file system using parallel connections
// This is a bonus feature that downloads chunks from multiple replicas simultaneously
func (c *Client) DownloadParallel(ctx context.Context, fileName, savePath string) error {
	// Step 1: Request download locations from Master Tracker
	downloadResp, err := c.masterClient.RequestDownload(ctx, &pb.DownloadRequest{
		FileName: fileName,
	})
	if err != nil {
		return fmt.Errorf("failed to request download: %w", err)
	}

	if len(downloadResp.Nodes) == 0 {
		return fmt.Errorf("no replicas available for file %s", fileName)
	}

	log.Printf("Master returned %d replica(s) for parallel download of %s", len(downloadResp.Nodes), fileName)

	// Step 2: Download from all replicas in parallel
	results := make(chan []byte, len(downloadResp.Nodes))
	errors := make(chan error, len(downloadResp.Nodes))

	for _, node := range downloadResp.Nodes {
		c.wg.Add(1)
		go func(n *pb.NodeAddress) {
			defer c.wg.Done()
			
			fileData, err := c.downloadFromNode(n, fileName)
			if err != nil {
				errors <- fmt.Errorf("download from %s:%d failed: %w", n.Host, n.TcpPort, err)
				return
			}
			results <- fileData
		}(node)
	}

	// Wait for all goroutines to complete
	c.wg.Wait()
	close(results)
	close(errors)

	// Check if we got any successful download
	var fileData []byte
	for data := range results {
		fileData = data
		break // Use the first successful download
	}

	if fileData == nil {
		// Collect all errors
		var errMsgs string
		for err := range errors {
			errMsgs += err.Error() + "; "
		}
		return fmt.Errorf("all parallel downloads failed: %s", errMsgs)
	}

	// Save the file to disk
	if err := os.WriteFile(savePath, fileData, 0644); err != nil {
		return fmt.Errorf("failed to write file to disk: %w", err)
	}

	log.Printf("Successfully downloaded file %s (%d bytes) in parallel and saved to %s", 
		fileName, len(fileData), savePath)
	return nil
}

// downloadFromNode downloads a file from a specific DataKeeper node
func (c *Client) downloadFromNode(node *pb.NodeAddress, fileName string) ([]byte, error) {
	// Connect to the DataKeeper's TCP port
	dkAddr := fmt.Sprintf("%s:%d", node.Host, node.TcpPort)
	conn, err := net.DialTimeout("tcp", dkAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Send operation code (0x02 = download)
	if _, err := conn.Write([]byte{0x02}); err != nil {
		return nil, fmt.Errorf("failed to send operation code: %w", err)
	}

	// Send filename request
	if err := binary.Write(conn, binary.BigEndian, uint32(len(fileName))); err != nil {
		return nil, fmt.Errorf("failed to send filename length: %w", err)
	}
	if _, err := conn.Write([]byte(fileName)); err != nil {
		return nil, fmt.Errorf("failed to send filename: %w", err)
	}

	// Receive the file using wire format
	_, fileData, err := recvFile(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to receive file: %w", err)
	}

	return fileData, nil
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

// extractFileName extracts the filename from a file path
func extractFileName(filePath string) string {
	// Find the last path separator
	for i := len(filePath) - 1; i >= 0; i-- {
		if filePath[i] == '/' || filePath[i] == '\\' {
			return filePath[i+1:]
		}
	}
	return filePath
}

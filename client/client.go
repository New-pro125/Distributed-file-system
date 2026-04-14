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

func (c *Client) Close() error {
	if c.masterConn != nil {
		return c.masterConn.Close()
	}
	return nil
}

func (c *Client) Upload(ctx context.Context, filePath string) error {
	// Open file for streaming instead of loading into memory
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	
	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	fileName := extractFileName(filePath)

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

	dkAddr := fmt.Sprintf("%s:%d", uploadResp.DataKeeper.Host, uploadResp.DataKeeper.TcpPort)
	conn, err := net.DialTimeout("tcp", dkAddr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to data keeper: %w", err)
	}
	defer conn.Close()
	
	// Set deadline for large file transfers
	conn.SetDeadline(time.Now().Add(30 * time.Minute))

	if _, err := conn.Write([]byte{0x01}); err != nil {
		return fmt.Errorf("failed to send operation code: %w", err)
	}

	if err := sendFileStream(conn, fileName, file, fileInfo.Size()); err != nil {
		return fmt.Errorf("failed to send file: %w", err)
	}

	log.Printf("Successfully uploaded file %s (%d bytes) to %s", fileName, fileInfo.Size(), dkAddr)

	time.Sleep(500 * time.Millisecond)

	return nil
}

func (c *Client) Download(ctx context.Context, fileName, savePath string) error {

	downloadResp, err := c.masterClient.RequestDownload(ctx, &pb.DownloadRequest{
		FileName: fileName,
	})
	if err != nil {
		return fmt.Errorf("failed to request download: %w", err)
	}

	if len(downloadResp.Nodes) == 0 {
		return fmt.Errorf("no replicas available for file %s", fileName)
	}

	for idx, node := range downloadResp.Nodes {
		log.Printf("Attempting to download from replica %d: %s:%d", idx+1, node.Host, node.TcpPort)

		fileSize, err := c.downloadFromNodeToFile(node, fileName, savePath)
		if err != nil {
			log.Printf("Failed to download from %s:%d: %v", node.Host, node.TcpPort, err)
			continue
		}

		log.Printf("Successfully downloaded file %s (%d bytes) and saved to %s",
			fileName, fileSize, savePath)
		return nil
	}

	return fmt.Errorf("failed to download file from any replica")
}

func (c *Client) DownloadParallel(ctx context.Context, fileName, savePath string) error {

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

	c.wg.Wait()
	close(results)
	close(errors)

	var fileData []byte
	for data := range results {
		fileData = data
		break
	}

	if fileData == nil {

		var errMsgs string
		for err := range errors {
			errMsgs += err.Error() + "; "
		}
		return fmt.Errorf("all parallel downloads failed: %s", errMsgs)
	}

	if err := os.WriteFile(savePath, fileData, 0644); err != nil {
		return fmt.Errorf("failed to write file to disk: %w", err)
	}

	log.Printf("Successfully downloaded file %s (%d bytes) in parallel and saved to %s",
		fileName, len(fileData), savePath)
	return nil
}

// downloadFromNodeToFile streams file directly to disk instead of memory
func (c *Client) downloadFromNodeToFile(node *pb.NodeAddress, fileName, savePath string) (int64, error) {
	dkAddr := fmt.Sprintf("%s:%d", node.Host, node.TcpPort)
	conn, err := net.DialTimeout("tcp", dkAddr, 30*time.Second)
	if err != nil {
		return 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()
	
	// Set deadline for large file transfers
	conn.SetDeadline(time.Now().Add(30 * time.Minute))

	if _, err := conn.Write([]byte{0x02}); err != nil {
		return 0, fmt.Errorf("failed to send operation code: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, uint32(len(fileName))); err != nil {
		return 0, fmt.Errorf("failed to send filename length: %w", err)
	}
	if _, err := conn.Write([]byte(fileName)); err != nil {
		return 0, fmt.Errorf("failed to send filename: %w", err)
	}

	fileSize, err := recvFileToWriter(conn, savePath)
	if err != nil {
		return 0, fmt.Errorf("failed to receive file: %w", err)
	}

	return fileSize, nil
}

// Legacy function for parallel download (loads into memory for comparison)
func (c *Client) downloadFromNode(node *pb.NodeAddress, fileName string) ([]byte, error) {
	dkAddr := fmt.Sprintf("%s:%d", node.Host, node.TcpPort)
	conn, err := net.DialTimeout("tcp", dkAddr, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()
	
	// Set deadline for large file transfers
	conn.SetDeadline(time.Now().Add(30 * time.Minute))

	if _, err := conn.Write([]byte{0x02}); err != nil {
		return nil, fmt.Errorf("failed to send operation code: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, uint32(len(fileName))); err != nil {
		return nil, fmt.Errorf("failed to send filename length: %w", err)
	}
	if _, err := conn.Write([]byte(fileName)); err != nil {
		return nil, fmt.Errorf("failed to send filename: %w", err)
	}

	_, fileData, err := recvFile(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to receive file: %w", err)
	}

	return fileData, nil
}

// sendFileStream sends a file by streaming it chunk by chunk instead of loading into memory
func sendFileStream(conn net.Conn, name string, reader io.Reader, size int64) error {
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

	// Stream file data in chunks using io.Copy
	written, err := io.Copy(conn, reader)
	if err != nil {
		return fmt.Errorf("failed to stream file data: %w", err)
	}
	if written != size {
		return fmt.Errorf("incomplete transfer: wrote %d bytes, expected %d", written, size)
	}

	return nil
}

// recvFileToWriter streams file directly to disk instead of loading into memory
func recvFileToWriter(conn net.Conn, outputPath string) (int64, error) {
	var nameLen uint32
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		return 0, fmt.Errorf("failed to read name length: %w", err)
	}

	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return 0, fmt.Errorf("failed to read filename: %w", err)
	}

	var fileSize uint64
	if err := binary.Read(conn, binary.BigEndian, &fileSize); err != nil {
		return 0, fmt.Errorf("failed to read file size: %w", err)
	}

	// Create output file
	file, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Stream data directly to disk
	written, err := io.CopyN(file, conn, int64(fileSize))
	if err != nil {
		return 0, fmt.Errorf("failed to stream file data to disk: %w", err)
	}

	return written, nil
}

// Legacy function for parallel download that loads into memory
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

func extractFileName(filePath string) string {

	for i := len(filePath) - 1; i >= 0; i-- {
		if filePath[i] == '/' || filePath[i] == '\\' {
			return filePath[i+1:]
		}
	}
	return filePath
}

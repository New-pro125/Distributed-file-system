package client

import (
	"bufio"
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

const (
	// ChunkSize defines the buffer size for streaming operations (32MB)
	ChunkSize = 16 * 1024 * 1024
	// LogInterval defines how often to log progress (every 64MB)
	LogInterval = 16 * 1024 * 1024
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

	// Use buffered reader for better upload performance
	bufReader := bufio.NewReaderSize(file, ChunkSize)
	opLabel := fmt.Sprintf("uploading to %s", dkAddr)

	if err := sendFileStreamWithLabel(conn, fileName, bufReader, fileInfo.Size(), opLabel); err != nil {
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

	// Use streaming for parallel download too - first successful download wins
	type result struct {
		fileSize int64
		err      error
	}
	results := make(chan result, len(downloadResp.Nodes))

	for _, node := range downloadResp.Nodes {
		c.wg.Add(1)
		go func(n *pb.NodeAddress) {
			defer c.wg.Done()

			// Each goroutine tries to download to a temp file
			tempPath := savePath + ".tmp." + fmt.Sprintf("%s-%d", n.Host, n.TcpPort)
			fileSize, err := c.downloadFromNodeToFile(n, fileName, tempPath)
			if err != nil {
				os.Remove(tempPath) // Clean up temp file on error
				results <- result{0, fmt.Errorf("download from %s:%d failed: %w", n.Host, n.TcpPort, err)}
				return
			}
			results <- result{fileSize, nil}
		}(node)
	}

	c.wg.Wait()
	close(results)

	// Find first successful download
	var successSize int64
	var errMsgs string
	for res := range results {
		if res.err == nil && successSize == 0 {
			successSize = res.fileSize
		} else if res.err != nil {
			errMsgs += res.err.Error() + "; "
		}
	}

	if successSize == 0 {
		return fmt.Errorf("all parallel downloads failed: %s", errMsgs)
	}

	// Rename first successful temp file to final destination
	for _, node := range downloadResp.Nodes {
		tempPath := savePath + ".tmp." + fmt.Sprintf("%s-%d", node.Host, node.TcpPort)
		if _, err := os.Stat(tempPath); err == nil {
			if err := os.Rename(tempPath, savePath); err == nil {
				log.Printf("Successfully downloaded file %s (%d bytes) in parallel from %s:%d and saved to %s",
					fileName, successSize, node.Host, node.TcpPort, savePath)
				// Clean up other temp files
				for _, n := range downloadResp.Nodes {
					os.Remove(savePath + ".tmp." + fmt.Sprintf("%s-%d", n.Host, n.TcpPort))
				}
				return nil
			}
		}
	}

	return fmt.Errorf("failed to finalize parallel download")
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

// sendFileStreamWithLabel sends a file by streaming it chunk by chunk with a custom operation label
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

	// Stream file data in chunks with progress logging
	written, err := streamWithProgress(conn, reader, size, name, opLabel)
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
	fileName := string(nameBuf)

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

	remoteAddr := conn.RemoteAddr().String()
	opLabel := fmt.Sprintf("downloading from %s", remoteAddr)

	// Stream data directly to disk with progress logging
	written, err := streamWithProgress(file, conn, int64(fileSize), fileName, opLabel)
	if err != nil {
		return 0, fmt.Errorf("failed to stream file data to disk: %w", err)
	}

	return written, nil
}

// streamWithProgress copies data in chunks with progress logging at intervals
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
			
			// Log progress only at meaningful intervals (every LogInterval bytes)
			if totalWritten-lastLoggedAt >= LogInterval || readErr == io.EOF {
				logNum++
				progress := float64(totalWritten) / float64(totalSize) * 100
				log.Printf("[%s] %s: progress update %d - %d MB transferred (%.2f%% complete)", 
					operation, fileName, logNum, totalWritten/(1024*1024), progress)
				lastLoggedAt = totalWritten
			}
		}
		
		if readErr == io.EOF {
			// Log final completion if not already logged
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



func extractFileName(filePath string) string {

	for i := len(filePath) - 1; i >= 0; i-- {
		if filePath[i] == '/' || filePath[i] == '\\' {
			return filePath[i+1:]
		}
	}
	return filePath
}

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
	"sync/atomic"
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
	rrIdx uint32
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

	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
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
	conn, err := net.DialTimeout("tcp", dkAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to data keeper: %w", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{0x01}); err != nil {
		return fmt.Errorf("failed to send operation code: %w", err)
	}

	if err := sendFile(conn, fileName, fileData); err != nil {
		return fmt.Errorf("failed to send file: %w", err)
	}

	log.Printf("Successfully uploaded file %s (%d bytes) to %s", fileName, len(fileData), dkAddr)

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

	nodesCount := len(downloadResp.Nodes)
	startIdx := int(atomic.AddUint32(&c.rrIdx, 1)) % nodesCount
	for i := 0; i < nodesCount; i++ {
		idx := (startIdx + i) % nodesCount
		node := downloadResp.Nodes[idx]
		log.Printf("Attempting to download from replica %d: %s:%d", idx+1, node.Host, node.TcpPort)

		fileData, err := c.downloadFromNode(node, fileName)
		if err != nil {
			log.Printf("Failed to download from %s:%d: %v", node.Host, node.TcpPort, err)
			continue
		}

		if err := os.WriteFile(savePath, fileData, 0644); err != nil {
			return fmt.Errorf("failed to write file to disk: %w", err)
		}

		log.Printf("Successfully downloaded file %s (%d bytes) and saved to %s",
			fileName, len(fileData), savePath)
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

func (c *Client) downloadFromNode(node *pb.NodeAddress, fileName string) ([]byte, error) {

	dkAddr := fmt.Sprintf("%s:%d", node.Host, node.TcpPort)
	conn, err := net.DialTimeout("tcp", dkAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

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

func extractFileName(filePath string) string {

	for i := len(filePath) - 1; i >= 0; i-- {
		if filePath[i] == '/' || filePath[i] == '\\' {
			return filePath[i+1:]
		}
	}
	return filePath
}

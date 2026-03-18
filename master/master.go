package master

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/New-pro125/distributed-file-system/gen/proto"
	"google.golang.org/grpc"
)

type FileRecord struct {
	FileName string
	NodeID   string
	FilePath string
	IsAlive  bool
}

type NodeRecord struct {
	Address  *pb.NodeAddress
	LastSeen time.Time
}

type MasterTracker struct {
	pb.UnimplementedMasterTrackerServer
	mu         sync.RWMutex
	table      map[string][]FileRecord
	nodes      map[string]*NodeRecord // tracking active nodes
	grpcServer *grpc.Server
	replTicker *time.Ticker
	done       chan struct{}
}

func NewMasterTracker() *MasterTracker {
	return &MasterTracker{
		table: make(map[string][]FileRecord),
		nodes: make(map[string]*NodeRecord),
		done:  make(chan struct{}),
	}
}

// Start gRPC server and replication loop
func (m *MasterTracker) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	m.grpcServer = grpc.NewServer()
	pb.RegisterMasterTrackerServer(m.grpcServer, m)

	m.replTicker = time.NewTicker(10 * time.Second)
	go m.replicationLoop()

	log.Printf("Master Tracker listening on port %d", port)
	return m.grpcServer.Serve(lis)
}

func (m *MasterTracker) Stop() {
	if m.replTicker != nil {
		m.replTicker.Stop()
	}
	close(m.done)
	if m.grpcServer != nil {
		m.grpcServer.GracefulStop()
	}
}

// RequestUpload selects the least loaded alive node
func (m *MasterTracker) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var selectedNode *pb.NodeAddress
	for _, node := range m.nodes {
		if time.Since(node.LastSeen) < 5*time.Second {
			// For simplicity, select the first alive node (could be improved with load balancing)
			selectedNode = node.Address
			break
		}
	}

	if selectedNode == nil {
		return nil, fmt.Errorf("no alive DataKeepers available")
	}

	return &pb.UploadResponse{
		DataKeeper: selectedNode,
	}, nil
}

// NotifyUploadDone adds the new file record to the lookup table
func (m *MasterTracker) NotifyUploadDone(ctx context.Context, req *pb.NotifyUploadRequest) (*pb.NotifyUploadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record := FileRecord{
		FileName: req.FileName,
		NodeID:   req.NodeId,
		FilePath: req.FilePath,
		IsAlive:  true,
	}

	m.table[req.FileName] = append(m.table[req.FileName], record)

	return &pb.NotifyUploadResponse{
		Success: true,
		Message: "File uploaded successfully",
	}, nil
}

// RequestDownload returns a slice of alive replicas
func (m *MasterTracker) RequestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	records, exists := m.table[req.FileName]
	if !exists || len(records) == 0 {
		return nil, fmt.Errorf("file not found")
	}

	var nodes []*pb.NodeAddress
	for _, record := range records {
		if node, err := m.getNodeInfo(record.NodeID); err == nil && time.Since(node.LastSeen) < 5*time.Second {
			nodes = append(nodes, node.Address)
		}
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("no alive replicas found")
	}

	return &pb.DownloadResponse{
		Nodes: nodes,
	}, nil
}

// Heartbeat updates IsAlive field and tracks last-seen timestamp
func (m *MasterTracker) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes[req.NodeId] = &NodeRecord{
		Address:  req.Address,
		LastSeen: time.Now(),
	}

	return &pb.HeartbeatResponse{
		Accepted: true,
	}, nil
}

func (m *MasterTracker) replicationLoop() {
	for {
		select {
		case <-m.done:
			return
		case <-m.replTicker.C:
			m.mu.Lock()
			for fileName, records := range m.table {
				aliveCount := 0
				for _, r := range records {
					if node, exists := m.nodes[r.NodeID]; exists && time.Since(node.LastSeen) < 5*time.Second {
						aliveCount++
					}
				}

				if aliveCount > 0 && aliveCount < 3 { // We need at least 3 alive replicas
					srcRecord := m.getSourceMachine(records)
					if srcRecord != nil {
						dstNodeID, dstNodeAddr := m.selectMachineToCopyTo(records)
						if dstNodeID != "" && dstNodeAddr != nil {
							// Trigger data transfer concurrently
							go m.notifyMachineDataTransfer(srcRecord.NodeID, dstNodeID, fileName)

							// Optimistically add it to the table to prevent multiple replication loops
							// queueing the same transfer.
							m.table[fileName] = append(m.table[fileName], FileRecord{
								FileName: fileName,
								NodeID:   dstNodeID,
								FilePath: srcRecord.FilePath, // Will be updated by NotifyUploadDone ideally
								IsAlive:  true,
							})
						}
					}
				}
			}
			m.mu.Unlock()
		}
	}
}

// getSourceMachine returns the first alive record for a given file
func (m *MasterTracker) getSourceMachine(records []FileRecord) *FileRecord {
	for _, record := range records {
		if node, exists := m.nodes[record.NodeID]; exists && time.Since(node.LastSeen) < 5*time.Second {
			return &record
		}
	}
	return nil
}

func (m *MasterTracker) getNodeInfo(nodeID string) (*NodeRecord, error) {
	node, exists := m.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node not found")
	}
	return node, nil
}

// selectMachineToCopyTo returns a live node ID and address that doesn't already have the file
func (m *MasterTracker) selectMachineToCopyTo(records []FileRecord) (string, *pb.NodeAddress) {
	hasFile := make(map[string]bool)
	for _, r := range records {
		hasFile[r.NodeID] = true
	}

	for id, node := range m.nodes {
		if !hasFile[id] && time.Since(node.LastSeen) < 5*time.Second {
			return id, node.Address
		}
	}

	return "", nil
}

// notifyMachineDataTransfer calls DataKeeperService.NotifyTransfer on src and dst
func (m *MasterTracker) notifyMachineDataTransfer(srcNodeID, dstNodeID, fileName string) {
	m.mu.RLock()
	srcNode, srcExists := m.nodes[srcNodeID]
	dstNode, dstExists := m.nodes[dstNodeID]
	m.mu.RUnlock()

	if !srcExists || !dstExists {
		return
	}

	log.Printf("Replicating file %s from %s to %s\n", fileName, srcNodeID, dstNodeID)

	// Stub
	_ = srcNode
	_ = dstNode
}

package mastertracker

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/New-pro125/distributed-file-system/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type FileRecord struct {
	FileName string
	NodeID   string
	FilePath string
	IsAlive  bool
}

type NodeInfo struct {
	NodeID   string
	Host     string
	TcpPort  int32
	GrpcPort int32
	LastSeen time.Time
	IsAlive  bool
}

type MasterTracker struct {
	pb.UnimplementedMasterTrackerServer

	mu    sync.RWMutex
	table map[string][]FileRecord
	nodes map[string]*NodeInfo

	grpcServer *grpc.Server
	replTicker *time.Ticker
	done       chan struct{}

	HeartbeatTimeout time.Duration

	ReplicationInterval time.Duration

	DesiredReplicas int
}

func New() *MasterTracker {
	return &MasterTracker{
		table:               make(map[string][]FileRecord),
		nodes:               make(map[string]*NodeInfo),
		done:                make(chan struct{}),
		HeartbeatTimeout:    5 * time.Second,
		ReplicationInterval: 10 * time.Second,
		DesiredReplicas:     3,
	}
}

func (m *MasterTracker) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	loadMap := make(map[string]int)
	for _, node := range m.nodes {
		if node.IsAlive {
			loadMap[node.NodeID] = 0
		}
	}
	if len(loadMap) == 0 {
		return nil, fmt.Errorf("no alive DataKeeper nodes available")
	}

	for _, records := range m.table {
		for _, rec := range records {
			if _, ok := loadMap[rec.NodeID]; ok {
				loadMap[rec.NodeID]++
			}
		}
	}

	var bestID string
	bestLoad := int(^uint(0) >> 1)
	for id, load := range loadMap {
		if load < bestLoad {
			bestLoad = load
			bestID = id
		}
	}

	node := m.nodes[bestID]
	log.Printf("[MasterTracker] RequestUpload(%s) → node %s at %s:%d",
		req.GetFileName(), bestID, node.Host, node.TcpPort)

	return &pb.UploadResponse{
		DataKeeper: &pb.NodeAddress{
			Host:    node.Host,
			TcpPort: node.TcpPort,
		},
	}, nil
}

func (m *MasterTracker) NotifyUploadDone(ctx context.Context, req *pb.NotifyUploadRequest) (*pb.NotifyUploadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec := FileRecord{
		FileName: req.GetFileName(),
		NodeID:   req.GetNodeId(),
		FilePath: req.GetFilePath(),
		IsAlive:  true,
	}
	m.table[req.GetFileName()] = append(m.table[req.GetFileName()], rec)

	log.Printf("[MasterTracker] NotifyUploadDone: file=%s node=%s path=%s",
		req.GetFileName(), req.GetNodeId(), req.GetFilePath())

	return &pb.NotifyUploadResponse{
		Success: true,
		Message: "file record added",
	}, nil
}

func (m *MasterTracker) RequestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	records, ok := m.table[req.GetFileName()]
	if !ok || len(records) == 0 {
		return nil, fmt.Errorf("file %q not found", req.GetFileName())
	}

	var addrs []*pb.NodeAddress
	seen := make(map[string]bool)
	for _, rec := range records {
		if seen[rec.NodeID] {
			continue
		}
		node, exists := m.nodes[rec.NodeID]
		if !exists || !node.IsAlive {
			continue
		}
		addrs = append(addrs, &pb.NodeAddress{
			Host:    node.Host,
			TcpPort: node.TcpPort,
		})
		seen[rec.NodeID] = true
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("no alive replicas for file %q", req.GetFileName())
	}

	log.Printf("[MasterTracker] RequestDownload(%s) → %d alive replica(s)",
		req.GetFileName(), len(addrs))

	return &pb.DownloadResponse{Nodes: addrs}, nil
}

func (m *MasterTracker) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeID := req.GetNodeId()
	addr := req.GetAddress()
	if addr == nil {
		return &pb.HeartbeatResponse{Accepted: false}, nil
	}

	info, exists := m.nodes[nodeID]
	if !exists {
		info = &NodeInfo{NodeID: nodeID}
		m.nodes[nodeID] = info
		log.Printf("[MasterTracker] New DataKeeper registered: %s (%s:%d)",
			nodeID, addr.GetHost(), addr.GetTcpPort())
	}

	info.Host = addr.GetHost()
	info.TcpPort = addr.GetTcpPort()
	info.GrpcPort = addr.GetGrpcPort()
	info.LastSeen = time.Now()
	info.IsAlive = true

	return &pb.HeartbeatResponse{Accepted: true}, nil
}

func (m *MasterTracker) replicationLoop() {
	m.replTicker = time.NewTicker(m.ReplicationInterval)
	defer m.replTicker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-m.replTicker.C:
			m.checkAndReplicate()
		}
	}
}

func (m *MasterTracker) checkAndReplicate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, node := range m.nodes {
		if node.IsAlive && now.Sub(node.LastSeen) > m.HeartbeatTimeout {
			log.Printf("[MasterTracker] Node %s timed out, marking dead", node.NodeID)
			node.IsAlive = false
		}
	}

	for fname, records := range m.table {
		for i := range records {
			if node, ok := m.nodes[records[i].NodeID]; ok {
				records[i].IsAlive = node.IsAlive
			} else {
				records[i].IsAlive = false
			}
		}
		m.table[fname] = records
	}

	for fileName, records := range m.table {
		aliveCount := m.instanceCount(records)
		if aliveCount >= m.DesiredReplicas {
			continue
		}

		src := m.getSourceMachine(records)
		if src == nil {
			log.Printf("[MasterTracker] No alive source for file %s, Cannot be replicated", fileName)
			continue
		}

		needed := m.DesiredReplicas - aliveCount
		selectedTargets := make(map[string]bool)
		for i := 0; i < needed; i++ {
			dst := m.selectMachineToCopyTo(fileName, records, selectedTargets)
			if dst == nil {
				log.Printf("[MasterTracker] No available target node for file %s", fileName)
				break
			}
			selectedTargets[dst.NodeID] = true

			srcNode := m.nodes[src.NodeID]

			go m.notifyMachineDataTransfer(srcNode, dst, fileName, src.FilePath)
		}
	}
}

func (m *MasterTracker) instanceCount(records []FileRecord) int {
	count := 0
	seen := make(map[string]bool)
	for _, r := range records {
		if r.IsAlive && !seen[r.NodeID] {
			count++
			seen[r.NodeID] = true
		}
	}
	return count
}

func (m *MasterTracker) getSourceMachine(records []FileRecord) *FileRecord {
	for i := range records {
		if records[i].IsAlive {
			return &records[i]
		}
	}
	return nil
}

func (m *MasterTracker) selectMachineToCopyTo(fileName string, records []FileRecord, selected map[string]bool) *NodeInfo {
	holders := make(map[string]bool)
	for _, r := range records {
		holders[r.NodeID] = true
	}

	for _, node := range m.nodes {
		if node.IsAlive && !holders[node.NodeID] && !selected[node.NodeID] {
			return node
		}
	}
	return nil
}

func (m *MasterTracker) notifyMachineDataTransfer(src *NodeInfo, dst *NodeInfo, fileName, filePath string) {

	dstGrpcAddr := fmt.Sprintf("%s:%d", dst.Host, dst.GrpcPort)
	conn, err := grpc.NewClient(dstGrpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[MasterTracker] Failed to dial destination DataKeeper %s at %s: %v",
			dst.NodeID, dstGrpcAddr, err)
		return
	}
	defer conn.Close()

	client := pb.NewDataKeeperClient(conn)
	resp, err := client.NotifyTransfer(context.Background(), &pb.TransferRequest{
		Src: &pb.NodeAddress{
			Host:    src.Host,
			TcpPort: src.TcpPort,
		},
		Dst: &pb.NodeAddress{
			Host:    dst.Host,
			TcpPort: dst.TcpPort,
		},
		FileName: fileName,
		FilePath: filePath,
	})

	if err != nil {
		log.Printf("[MasterTracker] NotifyTransfer to %s failed: %v", dst.NodeID, err)
		return
	}
	log.Printf("[MasterTracker] NotifyTransfer to %s: success=%v msg=%s",
		dst.NodeID, resp.GetSuccess(), resp.GetMessage())
}

func (m *MasterTracker) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	m.grpcServer = grpc.NewServer()
	pb.RegisterMasterTrackerServer(m.grpcServer, m)

	go m.replicationLoop()

	log.Printf("[MasterTracker] Listening on :%d", port)
	return m.grpcServer.Serve(lis)
}

func (m *MasterTracker) Stop() {
	log.Println("[MasterTracker] Shutting down…")
	close(m.done)
	if m.grpcServer != nil {
		m.grpcServer.GracefulStop()
	}
}

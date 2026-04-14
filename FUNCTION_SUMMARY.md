# Distributed File System - Function Summary

This document summarizes all functions in the handwritten codebase (excluding generated protobuf Go files under `gen/proto`).

## 1) Core Constructs Used Across the Project

### 1.1 Go Structs

- `client.Client`
  - Fields: `masterConn *grpc.ClientConn`, `masterClient pb.MasterTrackerClient`, `wg sync.WaitGroup`, `errCh chan error`
  - Purpose: client-side orchestration for upload/download through master + datakeeper nodes.

- `cmd/client.Config`
  - Fields: `MASTER_ADDR string`
  - Purpose: environment-backed CLI configuration for client entrypoint.

- `datakeeper.DataKeeper`
  - Embeds: `pb.UnimplementedDataKeeperServer`
  - Fields: node identity/address, ports, storage path, master gRPC connection/client, heartbeat ticker, shutdown channel, mutex.
  - Purpose: stores files, serves TCP upload/download, handles replication RPC, heartbeats to master.

- `cmd/datakeeper.Config`
  - Fields: `DK_ID`, `DK_HOST`, `MASTER_ADDR`, `DK_STORAGE_DIR`, `DK_GRPC_PORT`, `DK_TCP_PORT`
  - Purpose: environment-backed CLI configuration for datakeeper entrypoint.

- `mastertracker.FileRecord`
  - Fields: `FileName`, `NodeID`, `FilePath`, `IsAlive`
  - Purpose: one file replica record in the master table.

- `mastertracker.NodeInfo`
  - Fields: `NodeID`, `Host`, `TcpPort`, `GrpcPort`, `LastSeen`, `IsAlive`
  - Purpose: master-side state for a datakeeper node.

- `mastertracker.MasterTracker`
  - Embeds: `pb.UnimplementedMasterTrackerServer`
  - Fields: sync lock, file table, nodes map, gRPC server, replication ticker, shutdown channel, heartbeat timeout, replication interval, desired replica count, round-robin counter.
  - Purpose: central metadata service; upload/download routing + liveness tracking + replication control.

- `cmd/mastertracker.Config`
  - Fields: `MASTER_PORT int`
  - Purpose: environment-backed CLI configuration for master entrypoint.

### 1.2 Protobuf Constructs

#### Services

- `MasterTracker`
  - `RequestUpload(UploadRequest) returns (UploadResponse)`
  - `NotifyUploadDone(NotifyUploadRequest) returns (NotifyUploadResponse)`
  - `RequestDownload(DownloadRequest) returns (DownloadResponse)`
  - `Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)`

- `DataKeeper`
  - `NotifyTransfer(TransferRequest) returns (TransferResponse)`

#### Messages

- `NodeAddress { host, tcp_port, grpc_port }`
- `UploadRequest { file_name }`
- `UploadResponse { data_keeper }`
- `NotifyUploadRequest { file_name, node_id, file_path }`
- `NotifyUploadResponse { success, message }`
- `DownloadRequest { file_name }`
- `DownloadResponse { nodes[] }`
- `HeartbeatRequest { node_id, address }`
- `HeartbeatResponse { accepted }`
- `TransferRequest { src, dst, file_name, file_path }`
- `TransferResponse { success, message }`

## 2) Function Summary

## `client/client.go`

### `New(masterAddr string) (*Client, error)`

- Creates gRPC client connection to the master tracker.
- Initializes `Client` with typed protobuf client and buffered error channel.

### `(c *Client) Close() error`

- Closes underlying master gRPC connection when present.

### `(c *Client) Upload(ctx context.Context, filePath string) error`

- Reads local file bytes and derives filename.
- Calls `MasterTracker.RequestUpload` to select destination datakeeper.
- Opens TCP connection to selected datakeeper.
- Sends op code `0x01` (upload), then transmits file payload via `sendFile`.

### `(c *Client) Download(ctx context.Context, fileName, savePath string) error`

- Calls `MasterTracker.RequestDownload` for available replicas.
- Tries replicas sequentially using `downloadFromNode` until one succeeds.
- Writes downloaded bytes to `savePath`.

### `(c *Client) DownloadParallel(ctx context.Context, fileName, savePath string) error`

- Calls `MasterTracker.RequestDownload` for replica list.
- Spawns one goroutine per replica and attempts concurrent fetch.
- Uses first successful result, aggregates errors if all fail.
- Saves winning payload to `savePath`.

### `(c *Client) downloadFromNode(node *pb.NodeAddress, fileName string) ([]byte, error)`

- Opens TCP connection to a datakeeper.
- Sends op code `0x02` (download) and filename.
- Receives file payload via `recvFile` and returns bytes.

### `sendFile(conn net.Conn, name string, data []byte) error`

- Wire format writer: filename length, filename bytes, file size, file bytes.

### `recvFile(conn net.Conn) (name string, data []byte, err error)`

- Wire format reader: reads filename length/name, file size, and file bytes.

### `extractFileName(filePath string) string`

- Manual basename extraction by scanning for `/` or `\\`.

## `datakeeper/datakeeper.go`

### `New(id, host string, grpcPort, tcpPort int32, storageDir, masterAddr string) (*DataKeeper, error)`

- Ensures storage directory exists.
- Connects to master tracker via gRPC.
- Constructs `DataKeeper` with heartbeat ticker and shutdown channel.

### `(dk *DataKeeper) Start() error`

- Performs startup heartbeat check to verify master reachability.
- Launches background workers: existing-file scan, gRPC server, TCP server, heartbeat loop.

### `(dk *DataKeeper) Stop()`

- Signals shutdown, stops ticker, closes master connection.

### `(dk *DataKeeper) scanAndNotifyExistingFiles()`

- Scans local storage directory after short delay.
- For each existing file, calls `NotifyUploadDone` to register metadata with master.

### `(dk *DataKeeper) startGRPCServer()`

- Starts gRPC listener and serves `DataKeeper` RPCs (`NotifyTransfer`).

### `(dk *DataKeeper) startTCPServer()`

- Starts TCP listener for file transfer protocol.
- Accept loop dispatches each connection to `handleTCPConnection`.

### `(dk *DataKeeper) handleTCPConnection(conn net.Conn)`

- Reads 1-byte op code and routes:
  - `0x01` -> `handleUpload`
  - `0x02` -> `handleDownload`

### `(dk *DataKeeper) handleUpload(conn net.Conn)`

- Receives file over TCP via `recvFile`.
- Persists file under storage directory.
- Notifies master with `NotifyUploadDone`.

### `(dk *DataKeeper) handleDownload(conn net.Conn)`

- Reads requested filename from TCP stream.
- Loads file from disk and returns bytes using `sendFile`.

### `(dk *DataKeeper) heartbeatLoop()`

- On ticker interval, sends heartbeat to master.
- Logs failures or rejections.

### `(dk *DataKeeper) sendHeartbeat(timeout time.Duration) (*pb.HeartbeatResponse, error)`

- Builds timed context.
- Calls `MasterTracker.Heartbeat` with node identity and current address.

### `(dk *DataKeeper) NotifyTransfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error)`

- RPC entry for replication commands.
- If current node matches source, pushes file (`handleSourceTransfer`).
- If current node matches destination, pulls file (`handleDestinationTransfer`).
- Otherwise rejects request with explanatory response.

### `(dk *DataKeeper) handleSourceTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error)`

- Reads local replica from disk.
- Connects to destination over TCP, sends upload op code `0x01`, pushes file via `sendFile`.

### `(dk *DataKeeper) handleDestinationTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error)`

- Connects to source over TCP and requests file using download op code `0x02`.
- Receives and stores file locally.
- Asynchronously notifies master with `NotifyUploadDone` to register new replica.

### `sendFile(conn net.Conn, name string, data []byte) error`

- Same TCP wire-format writer used by datakeeper side.

### `recvFile(conn net.Conn) (name string, data []byte, err error)`

- Same TCP wire-format reader used by datakeeper side.

## `mastertracker/mastertracker.go`

### `New() *MasterTracker`

- Allocates metadata maps/channels and default replication/liveness settings.

### `(m *MasterTracker) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error)`

- Read-locks state.
- Computes alive-node load from replica table.
- Picks least-loaded alive datakeeper.
- Returns its `NodeAddress` for upload target.

### `(m *MasterTracker) NotifyUploadDone(ctx context.Context, req *pb.NotifyUploadRequest) (*pb.NotifyUploadResponse, error)`

- Write-locks state.
- Appends replica record (`FileRecord`) to file table.
- Returns success acknowledgment.

### `(m *MasterTracker) RequestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error)`

- Read-locks state.
- Collects unique alive nodes holding requested file.
- Applies round-robin rotation to diversify first replica selection.
- Returns ordered node list.

### `(m *MasterTracker) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)`

- Write-locks nodes map.
- Validates incoming address.
- Registers unknown node IDs.
- Refreshes host/ports/last-seen and marks node alive.

### `(m *MasterTracker) replicationLoop()`

- Runs periodic replication checks using ticker.
- Stops when shutdown channel is closed.

### `(m *MasterTracker) checkAndReplicate()`

- Marks timed-out nodes dead based on `HeartbeatTimeout`.
- Syncs `FileRecord.IsAlive` from node liveness.
- For under-replicated files:
  - selects alive source replica,
  - selects destination nodes not already holding file,
  - asynchronously issues transfer requests.

### `(m *MasterTracker) instanceCount(records []FileRecord) int`

- Counts unique alive node holders for a file.

### `(m *MasterTracker) getSourceMachine(records []FileRecord) *FileRecord`

- Returns first alive replica record for use as replication source.

### `(m *MasterTracker) selectMachineToCopyTo(fileName string, records []FileRecord, selected map[string]bool) *NodeInfo`

- Chooses alive node that does not already hold file and was not picked in current iteration.

### `(m *MasterTracker) notifyMachineDataTransfer(src *NodeInfo, dst *NodeInfo, fileName, filePath string)`

- Dials destination datakeeper gRPC endpoint.
- Invokes `DataKeeper.NotifyTransfer` with source/destination addresses and file metadata.

### `(m *MasterTracker) Start(port int) error`

- Starts master gRPC listener.
- Registers RPC service implementation.
- Launches background replication loop.
- Serves until shutdown/error.

### `(m *MasterTracker) Stop()`

- Signals replication loop termination.
- Gracefully stops gRPC server.

## `cmd/client/main.go`

### `main()`

- Loads env (`.env`) and CLI flags for client mode.
- Creates client connection to master.
- Routes command:
  - `upload` -> `Client.Upload`
  - `download` -> `Client.Download` or `Client.DownloadParallel`
- Applies timeout context and prints user-facing status.

## `cmd/datakeeper/main.go`

### `main()`

- Loads env and CLI flags for datakeeper identity/network/storage.
- Validates host and required args.
- Auto-selects available gRPC/TCP ports (up to bounded attempts).
- Creates and starts `DataKeeper` service.
- Waits for termination signal and performs graceful stop.

## `cmd/mastertracker/main.go`

### `main()`

- Loads env and CLI flags for master port.
- Creates `MasterTracker` instance.
- Installs signal handler for graceful shutdown.
- Searches for available port and starts master service.

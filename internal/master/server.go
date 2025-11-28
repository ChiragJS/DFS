package master

import (
	"context"
	"dfs/dfs/masterpb"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type MasterServer struct {
	masterpb.UnimplementedMasterServiceServer
	mu           sync.Mutex
	ChunkServers map[string]*ChunkServerInfo
	Files        map[string]*FileMetaData
	Chunks       map[string]*ChunkInfo
}

type ChunkServerInfo struct {
	Address       string
	FreeStorage   int64
	Chunks        map[string]bool
	LastHeartbeat time.Time
}

type FileMetaData struct {
	FileName string
	Chunks   []string
}

type ChunkInfo struct {
	ChunkID  string
	Replicas map[string]bool
}

func NewMasterServer() *MasterServer {
	return &MasterServer{
		ChunkServers: make(map[string]*ChunkServerInfo),
		Files:        make(map[string]*FileMetaData),
		Chunks:       make(map[string]*ChunkInfo),
	}
}

/*
GetFileInfo(context.Context, *GetFileInfoRequest) (*GetFileInfoResponse, error)
AllocateChunk(context.Context, *AllocateChunkRequest) (*AllocateChunkResponse, error)
RegisterChunkServer(context.Context, *RegisterChunkServerRequest) (*RegisterChunkServerResponse, error)
Heartbeat(grpc.BidiStreamingServer[HeartbeatRequest, HeartbeatResponse]) error
*/

func (ms *MasterServer) RegisterChunkServer(ctx context.Context, req *masterpb.RegisterChunkServerRequest) (*masterpb.RegisterChunkServerResponse, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()
	chunkMap := make(map[string]bool)
	for _, chunk := range req.GetChunks() {
		chunkMap[chunk] = true
	}

	serverInfo := &ChunkServerInfo{
		Address:       req.GetServerAddress(),
		FreeStorage:   req.GetFreeStorage(),
		Chunks:        chunkMap,
		LastHeartbeat: time.Now().UTC(),
	}

	ms.ChunkServers[serverInfo.Address] = serverInfo

	for _, chunk := range req.GetChunks() {
		if value, ok := ms.Chunks[chunk]; ok {
			value.Replicas[req.GetServerAddress()] = true
		} else {
			chunkInfo := &ChunkInfo{
				ChunkID:  chunk,
				Replicas: make(map[string]bool),
			}
			chunkInfo.Replicas[req.GetServerAddress()] = true
			ms.Chunks[chunk] = chunkInfo
		}
	}

	return &masterpb.RegisterChunkServerResponse{Success: true}, nil
}

func (ms *MasterServer) Heartbeat(stream grpc.BidiStreamingServer[masterpb.HeartbeatRequest, masterpb.HeartbeatResponse]) error {

	for {

		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
		//msg --> chunks , freeStorage , server Address
		ms.mu.Lock()

		if _, ok := ms.ChunkServers[msg.GetServerAddress()]; !ok {
			return fmt.Errorf("Server Not Registered")
		}

		ms.ChunkServers[msg.GetServerAddress()].FreeStorage = msg.GetFreeStorage()
		ms.ChunkServers[msg.GetServerAddress()].LastHeartbeat = time.Now()

		chunks := make(map[string]bool)

		for _, chunk := range msg.Chunks {
			chunks[chunk] = true
		}

		// Identify chunks missing from this server (delete tasks)

		// Update global chunk -> replica mapping

		// Determine replication tasks

		ms.mu.Unlock()
	}
	return fmt.Errorf("Some error for now")
}

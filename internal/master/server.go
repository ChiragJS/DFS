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

// hardcoding replication factor for now
var REPLICATION_FACTOR int = 2

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
	FileName string
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

		// Check if the server address exists
		serverAddress := msg.GetServerAddress()

		ms.mu.Lock()

		if _, ok := ms.ChunkServers[serverAddress]; !ok {
			ms.mu.Unlock()
			return fmt.Errorf("Server Not Registered")
		}
		ms.ChunkServers[serverAddress].FreeStorage = msg.GetFreeStorage()
		ms.ChunkServers[serverAddress].LastHeartbeat = time.Now().UTC()

		serverChunks := make(map[string]bool)

		for _, chunk := range msg.GetChunks() {
			serverChunks[chunk] = true
		}

		// find missing chunks
		missingChunks := make([]string, 0)
		for chunk := range ms.ChunkServers[serverAddress].Chunks {

			if _, ok := serverChunks[chunk]; !ok {
				missingChunks = append(missingChunks, chunk)
			}

		}

		newChunks := make([]string, 0)

		for chunk := range serverChunks {
			if _, ok := ms.ChunkServers[serverAddress].Chunks[chunk]; !ok {
				newChunks = append(newChunks, chunk)
			}
		}

		rep1 := ms.processMissingChunks(serverAddress, missingChunks)

		rep2, deleteTask := ms.processNewChunks(serverAddress, newChunks)

		rep1 = append(rep1, rep2...)

		replicationTask := ms.buildReplicationTasks(serverAddress, rep1)

		ms.ChunkServers[serverAddress].Chunks = serverChunks
		ms.mu.Unlock()
		if err := stream.Send(&masterpb.HeartbeatResponse{
			ReplicationTasks: replicationTask,
			DeleteTasks:      deleteTask,
		}); err != nil {
			return err
		}

	}
	return nil

}

func (ms *MasterServer) processMissingChunks(serverAddress string, missingChunks []string) []string {

	rep := make([]string, 0)
	for _, chunk := range missingChunks {
		chunkInfo, ok := ms.Chunks[chunk]
		if !ok {
			continue
		}
		delete(chunkInfo.Replicas, serverAddress)

		// if no file -> skip replication
		if chunkInfo.FileName == "" || ms.Files[chunkInfo.FileName] == nil {
			if len(chunkInfo.Replicas) == 0 {
				delete(ms.Chunks, chunk)
			}
			continue
		}

		// if no replicas left -> metadata becomes invalid
		if len(chunkInfo.Replicas) == 0 {
			delete(ms.Chunks, chunk)
			continue
		}
		if len(chunkInfo.Replicas) < REPLICATION_FACTOR {
			rep = append(rep, chunk)
		}
	}

	return rep
}

func (ms *MasterServer) processNewChunks(serverAddress string, newChunks []string) ([]string, []*masterpb.DeleteTask) {
	rep, deleteTask := make([]string, 0), make([]*masterpb.DeleteTask, 0)

	for _, chunk := range newChunks {
		chunkInfo, ok := ms.Chunks[chunk]
		if !ok {
			deleteTask = append(deleteTask, &masterpb.DeleteTask{ChunkId: chunk})
			continue
		}

		if chunkInfo.FileName == "" || ms.Files[chunkInfo.FileName] == nil {
			deleteTask = append(deleteTask, &masterpb.DeleteTask{ChunkId: chunk})
			continue
		}

		chunkInfo.Replicas[serverAddress] = true

		if len(chunkInfo.Replicas) > REPLICATION_FACTOR {
			deleteTask = append(deleteTask, &masterpb.DeleteTask{ChunkId: chunk})
			continue
		}
		if len(chunkInfo.Replicas) < REPLICATION_FACTOR {
			rep = append(rep, chunk)
		}

	}
	return rep, deleteTask

}
func (ms *MasterServer) buildReplicationTasks(serverAddress string, rep []string) []*masterpb.ReplicationTask {
	return make([]*masterpb.ReplicationTask, 0)
}

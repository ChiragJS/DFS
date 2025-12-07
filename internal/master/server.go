package master

import (
	"context"
	"dfs/dfs/masterpb"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// hardcoding replication factor for now
var REPLICATION_FACTOR int = 2

const CHUNK_SIZE int64 = 64 * 1024 * 1024 // 64MB

type MasterServer struct {
	masterpb.UnimplementedMasterServiceServer
	mu               sync.Mutex
	ChunkServers     map[string]*ChunkServerInfo
	Files            map[string]*FileMetaData
	Chunks           map[string]*ChunkInfo
	ReplicationWorks map[string][]*ReplicationWork
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

type ReplicationWork struct {
	TargetAddress string
	ChunkID       string
}

func NewMasterServer() *MasterServer {
	return &MasterServer{
		ChunkServers:     make(map[string]*ChunkServerInfo),
		Files:            make(map[string]*FileMetaData),
		Chunks:           make(map[string]*ChunkInfo),
		ReplicationWorks: make(map[string][]*ReplicationWork),
	}
}

func (ms *MasterServer) Start() error {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	masterpb.RegisterMasterServiceServer(grpcServer, ms)
	fmt.Println("MasterServer listening on port 8000")
	return grpcServer.Serve(lis)

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
	fmt.Println("Success registeration of chunk server %s\n", req.GetServerAddress())
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
		fmt.Printf("Heart beat from server : %s \n", msg.GetServerAddress())
		// Check if the server address exists
		serverAddress := msg.GetServerAddress()

		ms.mu.Lock()

		if _, ok := ms.ChunkServers[serverAddress]; !ok {
			ms.mu.Unlock()
			return fmt.Errorf("server not registered")
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

		ms.buildReplicationTasks(rep1)

		replicationTask := make([]*masterpb.ReplicationTask, 0)
		for _, work := range ms.ReplicationWorks[serverAddress] {
			replicationTask = append(replicationTask, &masterpb.ReplicationTask{
				ChunkId:       work.ChunkID,
				TargetAddress: work.TargetAddress,
			})
		}
		// clear assigned tasks
		ms.ChunkServers[serverAddress].Chunks = serverChunks
		ms.mu.Unlock()
		if err := stream.Send(&masterpb.HeartbeatResponse{
			ReplicationTasks: replicationTask,
			DeleteTasks:      deleteTask,
		}); err != nil {
			return err
		}
		ms.mu.Lock()
		ms.ReplicationWorks[serverAddress] = make([]*ReplicationWork, 0)
		ms.mu.Unlock()
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
func (ms *MasterServer) buildReplicationTasks(rep []string) {

	const liveThreshold = 30 * time.Second
	// local snapshot of free server storage
	serverStorage := make(map[string]int64)
	for addr, info := range ms.ChunkServers {
		serverStorage[addr] = info.FreeStorage
	}

	type srv struct {
		addr  string
		free  int64
		alive bool
	}
	servers := make([]srv, 0, len(ms.ChunkServers))

	for addr, info := range ms.ChunkServers {
		servers = append(servers, srv{
			addr:  addr,
			free:  info.FreeStorage,
			alive: time.Since(info.LastHeartbeat) <= liveThreshold,
		})
	}

	// highest free storage first
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].free > servers[j].free
	})

	for _, chunkID := range rep {

		ci, ok := ms.Chunks[chunkID]
		if !ok {
			continue
		}

		replicasNeeded := REPLICATION_FACTOR - len(ci.Replicas)
		if replicasNeeded <= 0 {
			continue
		}
		// source selection
		var source string
		for replica := range ci.Replicas {
			if info := ms.ChunkServers[replica]; time.Since(info.LastHeartbeat) <= liveThreshold {
				source = replica
				break
			}
		}
		if source == "" {
			continue // no alive replica to supply the chunk
		}
		// target selection
		targets := make([]string, 0)

		for _, s := range servers {

			if replicasNeeded == 0 {
				break
			}

			if !s.alive {
				continue
			}

			if _, alreadyReplica := ci.Replicas[s.addr]; alreadyReplica {
				continue
			}

			if serverStorage[s.addr] < CHUNK_SIZE {
				continue
			}

			targets = append(targets, s.addr)
			serverStorage[s.addr] -= CHUNK_SIZE // simulation
			replicasNeeded--
		}
		// add tasks to ReplicationWorks
		for _, dest := range targets {
			work := &ReplicationWork{
				TargetAddress: dest,
				ChunkID:       chunkID,
			}
			ms.ReplicationWorks[source] = append(ms.ReplicationWorks[source], work)
		}
	}
}

func (ms *MasterServer) GetFileInfo(ctx context.Context, fileInfo *masterpb.GetFileInfoRequest) (*masterpb.GetFileInfoResponse, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	fileMetaData, exists := ms.Files[fileInfo.GetFileName()]
	if !exists {
		return &masterpb.GetFileInfoResponse{
			FileInfo: make([]*masterpb.FileChunkInfo, 0),
		}, fmt.Errorf("file does not exist")
	}

	chunks := make([]*masterpb.FileChunkInfo, 0)

	for _, chunkID := range fileMetaData.Chunks {
		chunk, exists := ms.Chunks[chunkID]
		if !exists {
			continue
		}
		replicas := make([]string, 0)
		for replica := range chunk.Replicas {
			replicas = append(replicas, replica)
		}

		chunks = append(chunks, &masterpb.FileChunkInfo{
			ChunkId:        chunkID,
			ReplicaServers: replicas,
		})
	}

	return &masterpb.GetFileInfoResponse{
		FileInfo: chunks,
	}, nil
}

func (ms *MasterServer) AllocateChunk(ctx context.Context, request *masterpb.AllocateChunkRequest) (*masterpb.AllocateChunkResponse, error) {

	// gets the file name and chunk index number as input
	// send the chunkId and the replica servers
	const liveThreshold = 30 * time.Second
	fileName := request.GetFileName()
	chunkIdx := request.GetChunkIndex()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	fileMetaData, exists := ms.Files[fileName]
	if !exists {
		fileMetaData = &FileMetaData{
			FileName: fileName,
			Chunks:   make([]string, 0),
		}
		ms.Files[fileName] = fileMetaData
	}

	chunkID := fileName + "_" + strconv.Itoa(int(chunkIdx))
	if _, exists := ms.Chunks[chunkID]; exists {
		return &masterpb.AllocateChunkResponse{
			ChunkId:        chunkID,
			ReplicaServers: make([]string, 0),
		}, fmt.Errorf(" chunk already exists ")
	}
	// alive servers

	validServers := make([]*ChunkServerInfo, 0)

	for _, serverInfo := range ms.ChunkServers {
		if time.Since(serverInfo.LastHeartbeat) > liveThreshold || serverInfo.FreeStorage < CHUNK_SIZE {
			continue
		}
		validServers = append(validServers, serverInfo)
	}

	if len(validServers) < REPLICATION_FACTOR {
		return &masterpb.AllocateChunkResponse{
			ChunkId:        chunkID,
			ReplicaServers: make([]string, 0),
		}, fmt.Errorf(" server count criteria not met ")
	}

	sort.Slice(validServers, func(i, j int) bool {
		return validServers[i].FreeStorage > validServers[j].FreeStorage
	})

	replicaServers := []string{}
	replicaServerMap := make(map[string]bool)

	for i := 0; i < REPLICATION_FACTOR; i++ {
		replicaServers = append(replicaServers, validServers[i].Address)
		replicaServerMap[replicaServers[i]] = true
		ms.ChunkServers[replicaServers[i]].Chunks[chunkID] = true
		ms.ChunkServers[replicaServers[i]].FreeStorage -= CHUNK_SIZE
	}

	fileMetaData.Chunks = append(fileMetaData.Chunks, chunkID)
	ms.Chunks[chunkID] = &ChunkInfo{
		FileName: fileName,
		ChunkID:  chunkID,
		Replicas: replicaServerMap,
	}

	return &masterpb.AllocateChunkResponse{
		ChunkId:        chunkID,
		ReplicaServers: replicaServers,
	}, nil
}

package chunkserver

import (
	"context"
	"dfs/dfs/chunkpb"
	"dfs/dfs/masterpb"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ChunkServer struct {
	chunkpb.UnimplementedChunkServiceServer
	myAddress     string
	masterAddress string
	storageDir    string

	mu              sync.Mutex
	chunks          []string
	replicationTask chan *masterpb.ReplicationTask
	deleteTask      chan *masterpb.DeleteTask
}

func NewChunkServer(myAddress, masterAddress, storageDir string) *ChunkServer {
	return &ChunkServer{
		myAddress:       myAddress,
		masterAddress:   masterAddress,
		storageDir:      storageDir,
		chunks:          make([]string, 0),
		replicationTask: make(chan *masterpb.ReplicationTask, 1000), // keep this in mind!!
		deleteTask:      make(chan *masterpb.DeleteTask, 1000),
	}
}

func (cs *ChunkServer) Start() error {

	if err := os.MkdirAll(cs.storageDir, 0755); err != nil {
		return err
	}
	lis, err := net.Listen("tcp", cs.myAddress)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	chunkpb.RegisterChunkServiceServer(grpcServer, cs)
	// implement a go routine for registering the chunk server // call the master server from there , by creating a connection
	cs.registerWithMaster()

	go cs.runHeartbeat()
	go cs.replicateAndDeleteTasks()
	go cs.refreshChunks()
	// the server will only start once it is registered on master
	// need a thread to monitor delete and replication --> will spawn two threads --> one will do replication , one will delete
	fmt.Println("ChunkServer listening on", cs.myAddress)
	return grpcServer.Serve(lis)
}

func (cs *ChunkServer) UploadChunk(
	stream grpc.ClientStreamingServer[chunkpb.ChunkData, chunkpb.UploadChunkStatus],
) error {

	//Checksum left
	var file *os.File
	var chunkId string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if file == nil {
			chunkId = msg.GetChunkId()
			filePath := filepath.Join(cs.storageDir, chunkId)

			file, err = os.Create(filePath)
			if err != nil {
				return err
			}

		}

		_, err = file.Write(msg.GetData())
		// handle partial writes , so maybe just remove the given file for now
		if err != nil {
			return err
		}

	}
	file.Sync()
	file.Close()
	cs.mu.Lock()
	cs.chunks = append(cs.chunks, chunkId)
	cs.mu.Unlock()
	return stream.SendAndClose(&chunkpb.UploadChunkStatus{Success: true})
}

func (cs *ChunkServer) DownloadChunk(
	req *chunkpb.DownloadChunkRequest,
	stream grpc.ServerStreamingServer[chunkpb.ChunkData],
) error {
	//Checksum left for now

	chunkId := req.GetChunkId()
	buff := make([]byte, 64*1024)
	filePath := filepath.Join(cs.storageDir, chunkId)
	file, err := os.Open(filePath)

	if err != nil {
		return err
	}
	defer file.Close()
	var seqNo int32 = 0
	for {
		n, err := file.Read(buff)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		chunk := &chunkpb.ChunkData{
			SeqNo:    int32(seqNo),
			ChunkId:  chunkId,
			Data:     buff[:n],
			Checksum: 0,
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
		seqNo++
	}
	return nil
}

func (cs *ChunkServer) ReplicateChunkToTarget(chunkId, targetAddress string) error {
	fmt.Println("Start replicate")
	filePath := filepath.Join(cs.storageDir, chunkId)
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	buff := make([]byte, 64*1024)

	conn, err := grpc.NewClient(targetAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}
	defer conn.Close()

	client := chunkpb.NewChunkServiceClient(conn)
	var seqNo int32 = 0
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.UploadChunk(ctx)
	if err != nil {
		return err
	}
	for {
		n, err := file.Read(buff)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		chunk := &chunkpb.ChunkData{
			SeqNo:    int32(seqNo),
			ChunkId:  chunkId,
			Data:     buff[:n],
			Checksum: 0,
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
		seqNo++
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("replication to %s failed", targetAddress)
	}
	fmt.Println("Replication done")
	return nil
}

// keeps on trying until registered for now
func (cs *ChunkServer) registerWithMaster() {
	for {
		conn, err := grpc.NewClient(
			cs.masterAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			fmt.Println("Connection failed, retrying:", err)
			time.Sleep(time.Second)
			continue
		}

		client := masterpb.NewMasterServiceClient(conn)

		free, err := disk_usage()
		if err != nil {
			fmt.Println("Disk usage error:", err)
			time.Sleep(time.Second)
			continue
		}
		cs.mu.Lock()
		chunks := append([]string(nil), cs.chunks...)
		cs.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		_, err = client.RegisterChunkServer(ctx, &masterpb.RegisterChunkServerRequest{
			ServerAddress: cs.myAddress,
			FreeStorage:   free,
			Chunks:        chunks,
		})

		conn.Close()

		if err != nil {
			fmt.Println("registeration failed , retrying:", err)
			time.Sleep(time.Second)
			continue
		}

		fmt.Println("successfully registered with master")
		cancel()
		return
	}

}

func (cs *ChunkServer) runHeartbeat() {
	// create a connection to master and every 5 seconds send the value
	var (
		conn *grpc.ClientConn
		err  error
	)
	for {
		conn, err = grpc.NewClient(cs.masterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println("Connection failed to master")
			// implement retry mechanism here
			continue
		}
		break
	}
	client := masterpb.NewMasterServiceClient(conn)
	for {
		ctx := context.Background()

		stream, err := client.Heartbeat(ctx)
		if err != nil {
			fmt.Printf("Error while calling heartbeat rpc %s\n", err)
			continue
		}
		wg := new(sync.WaitGroup)
		wg.Add(2)
		// now one read thread and one write thread

		// read thread
		go func() {
			defer wg.Done()
			for {
				msg, err := stream.Recv()
				if err == io.EOF {
					break
				}
				// no use of err here
				if err != nil {
					fmt.Printf("Error from reading thread of cs %s", cs.myAddress)
					fmt.Println(err)
					break // maybe ?? something to think of for now
				}
				deleteTask := msg.GetDeleteTasks()
				replicationTask := msg.GetReplicationTasks()

				// becomes blocking , when the channel is at capacity --> hence the streaming is blocked .. but its fine for now
				for _, task := range deleteTask {
					cs.deleteTask <- task
				}
				for _, task := range replicationTask {
					cs.replicationTask <- task
				}
			}
			// the implementation seems fine for now
		}()
		// write thread
		go func() {
			defer wg.Done()
			for {

				storage, err := disk_usage()
				if err != nil {
					fmt.Println("Disk Usage Error ")
					continue
				}

				cs.mu.Lock()
				chunks := append([]string(nil), cs.chunks...)
				cs.mu.Unlock()
				// gather the heartbeat response
				heartbeat := &masterpb.HeartbeatRequest{
					ServerAddress: cs.myAddress,
					FreeStorage:   storage,
					Chunks:        chunks,
				}

				err = stream.Send(heartbeat)
				if err != nil {
					fmt.Printf("Error in sending heartbeat from cs %s", cs.myAddress)
				}
				time.Sleep(time.Second * 5) // for now , need to check in the master service, what i have configured
				// idts there is a need to terminate this loop  .. for now!!
			}
		}()

		// use wait group , then the thread ends
		wg.Wait()
		// time.Sleep()
		conn.Close()
	}

}

func (cs *ChunkServer) replicateAndDeleteTasks() {

	// replicate task , this is an infinitely running routine , since the channel is never closed
	go func() {
		for task := range cs.replicationTask {
			// create a connection to a client
			if err := cs.ReplicateChunkToTarget(task.GetChunkId(), task.GetTargetAddress()); err != nil {
				fmt.Printf("Error in Replication %s\n", err)
			}
		}
	}()
	// delete task --> the channel never closes , so this is an infinitely running routine
	// althought it is fine , since it is a receiving thread
	go func() {
		for task := range cs.deleteTask {
			// delete these files from the disk
			filePath := filepath.Join(cs.storageDir, task.GetChunkId())
			if err := os.Remove(filePath); err != nil {
				fmt.Printf("Error while deleting file %s\n", err)
				continue
			}
			cs.mu.Lock()
			index := -1

			for idx, chunkId := range cs.chunks {
				if chunkId == task.GetChunkId() {
					index = idx
					break
				}
			}
			// use a mutex here maybe !
			// deletion trick ( doesn't preserver order )
			if index != -1 {
				cs.chunks[index] = cs.chunks[len(cs.chunks)-1]
				cs.chunks = cs.chunks[:len(cs.chunks)-1]
			}
			cs.mu.Unlock()

		}
	}()

}

func (cs *ChunkServer) refreshChunks() {

	for {

		chunks := make([]string, 0)

		filesInDir, err := os.ReadDir(cs.storageDir)
		if err != nil {
			fmt.Printf("Error while refreshing Chunk %s \n", err)
		}

		for _, files := range filesInDir {
			chunks = append(chunks, files.Name())
		}
		cs.mu.Lock()
		cs.chunks = chunks
		cs.mu.Unlock()
		time.Sleep(time.Second * 10)
	}
}

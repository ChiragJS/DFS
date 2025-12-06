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
	myAddress       string
	masterAddress   string
	storageDir      string
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
		replicationTask: make(chan *masterpb.ReplicationTask),
		deleteTask:      make(chan *masterpb.DeleteTask),
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
		if err != nil {
			return err
		}

	}
	file.Close()
	cs.chunks = append(cs.chunks, chunkId)
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

		chunks := cs.chunks

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

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
		return
	}

}

func (cs *ChunkServer) runHeartbeat() {
	// create a connection to master and every 5 seconds send the value

	for {
		conn, err := grpc.NewClient(cs.masterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println("Connection failed to master")
			continue
		}
		client := masterpb.NewMasterServiceClient(conn)
		ctx := context.Background()
		client.Heartbeat(ctx)
		storage, err := disk_usage()
		if err != nil {
			fmt.Println("Disk Usage Error ")
		}

		stream, err := client.Heartbeat(ctx)
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

				for _, task := range deleteTask {
					cs.deleteTask <- task
				}
				for _, task := range replicationTask {
					cs.replicationTask <- task
				}
			}
			// should i close the connection ?? idts for now
			// i should wait for these threads to complete in this routine , since it is non blocking ... think about it for now
			// need some condition to break out of this loop
		}()
		// write thread
		go func() {
			wg.Done()
			for {
				// gather the heartbeat response
				heartbeat := &masterpb.HeartbeatRequest{
					ServerAddress: cs.myAddress,
					FreeStorage:   storage,
					Chunks:        cs.chunks,
				}

				err := stream.Send(heartbeat)
				if err != nil {
					fmt.Printf("Error in sending heartbeat from cs %s", cs.myAddress)
				}
				time.Sleep(time.Second * 5) // for now , need to check in the master service, what i have configured
				// find the case to exit this infinite loop
			}
		}()

		// use wait group , then the thread ends
		wg.Wait()

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
	go func() {
		for task := range cs.deleteTask {
			// delete these files from the disk
			filePath := filepath.Join(cs.storageDir, task.GetChunkId())
			if err := os.Remove(filePath); err != nil {
				fmt.Printf("Error while deleting file %s\n", err)
				continue
			}
			var index int
			for idx, chunkId := range cs.chunks {
				if chunkId == task.GetChunkId() {
					index = idx
					break
				}
			}

			// deletion trick ( doesn't preserver order )
			cs.chunks[index] = cs.chunks[len(cs.chunks)-1]
			cs.chunks = cs.chunks[:len(cs.chunks)-1]

		}
	}()

}

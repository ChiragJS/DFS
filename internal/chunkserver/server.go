package chunkserver

import (
	"context"
	"dfs/dfs/chunkpb"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ChunkServer struct {
	chunkpb.UnimplementedChunkServiceServer
	myAddress     string
	masterAddress string
	storageDir    string
}

func NewChunkServer(myAddress, masterAddress, storageDir string) *ChunkServer {
	return &ChunkServer{
		myAddress:     myAddress,
		masterAddress: masterAddress,
		storageDir:    storageDir,
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

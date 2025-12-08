package main

import (
	"context"
	"dfs/dfs/chunkpb"
	"dfs/dfs/masterpb"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(":8000", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		fmt.Printf("Error creating master conn %s \n", err)
	}
	defer conn.Close()
	client := masterpb.NewMasterServiceClient(conn)

	resp, err := client.AllocateChunk(context.Background(), &masterpb.AllocateChunkRequest{
		FileName:   "testFile.txt",
		ChunkIndex: 2,
	})

	if err != nil {
		fmt.Printf("Error : %s", err)
		return
	}

	for _, serverAddress := range resp.GetReplicaServers() {

		uploadToChunkServer(serverAddress, resp.GetChunkId(), []byte("Hello World"))

	}

	resp1, err := client.GetFileInfo(context.Background(), &masterpb.GetFileInfoRequest{
		FileName: "testFile.txt",
	})

	if err != nil {
		fmt.Printf("Error while getting File Info %s \n", err)
	}
	fmt.Println(resp1)
}

func uploadToChunkServer(serverAddress, chunkID string, data []byte) {

	conn1, err1 := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err1 != nil {
		fmt.Printf("Error creating client conn %s \n", err1)
	}
	defer conn1.Close()
	client1 := chunkpb.NewChunkServiceClient(conn1)

	stream, err := client1.UploadChunk(context.Background())

	if err != nil {
		fmt.Printf("Error while calling rpc %s \n", err)
	}
	stream.Send(&chunkpb.ChunkData{
		ChunkId: chunkID,
		Data:    data,
	})

	resp, _ := stream.CloseAndRecv()
	fmt.Println(resp)
}

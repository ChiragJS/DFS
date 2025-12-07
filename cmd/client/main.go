package main

import (
	"context"
	"dfs/dfs/masterpb"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(":8000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}

	client := masterpb.NewMasterServiceClient(conn)

	resp, err := client.AllocateChunk(context.Background(), &masterpb.AllocateChunkRequest{
		FileName:   "testFile.txt",
		ChunkIndex: 0,
	})

	if err != nil {
		fmt.Errorf("Error : %s", err)
		return
	}

	fmt.Printf("ChunkId : ", resp.GetChunkId(), "Replica Servers", resp.GetReplicaServers())
}

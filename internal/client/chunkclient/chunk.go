package chunkclient

import (
	"context"
	"dfs/dfs/chunkpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ChunkClient struct {
	address string
	conn    *grpc.ClientConn
	client  chunkpb.ChunkServiceClient
}

func NewChunkClient(address string) (*ChunkClient, error) {

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client := chunkpb.NewChunkServiceClient(conn)
	return &ChunkClient{
		address: address,
		conn:    conn,
		client:  client,
	}, nil
}
func (cc *ChunkClient) UploadChunk(chunkID string) (grpc.ClientStream, error) {
	// getting the stream object again and again .. hmm something to optimize later
	stream, err := cc.client.UploadChunk(context.Background())
	if err != nil {
		return nil, err
	}
	return stream, err

	// job of this function is to return a stream object and let the caller use it for now
}

func (cc *ChunkClient) DownloadChunk(chunkID string) (grpc.ServerStreamingClient[chunkpb.ChunkData], error) {
	req := &chunkpb.DownloadChunkRequest{
		ChunkId: chunkID,
	}
	stream, err := cc.client.DownloadChunk(context.Background(), req)

	if err != nil {
		return nil, err
	}
	return stream, err
	// job of this function is to return a stream object and let the caller use it for now
}

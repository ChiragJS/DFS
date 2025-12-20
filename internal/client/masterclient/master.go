package masterclient

import (
	"context"
	"dfs/dfs/masterpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// makes the caller handler all the errors --> will decide a later time on how to handle errors globally

type MasterClient struct {
	address string
	conn    *grpc.ClientConn
	client  masterpb.MasterServiceClient
}

func (mc *MasterClient) AllocateChunk(fileName string, chunkIndex int32) (*masterpb.AllocateChunkResponse, error) {

	req := &masterpb.AllocateChunkRequest{
		FileName:   fileName,
		ChunkIndex: chunkIndex,
	}
	resp, err := mc.client.AllocateChunk(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp, nil

}

func (mc *MasterClient) GetFileInfo(fileName string) (*masterpb.GetFileInfoResponse, error) {
	fileInfoReq := &masterpb.GetFileInfoRequest{
		FileName: fileName,
	}

	resp, err := mc.client.GetFileInfo(context.Background(), fileInfoReq)
	if err != nil {
		return nil, err
	}
	return resp, nil

}

func NewMasterClient(address string) (*MasterClient, error) {

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client := masterpb.NewMasterServiceClient(conn)

	return &MasterClient{
		address: address,
		conn:    conn,
		client:  client, // storing the client here maybe check why the pointer is not being accepted
	}, nil

}

// connection closed ... so this instance is useless for now .. so create a new one , clean it up
// think about a better way to implement
func (mc *MasterClient) Close() error {
	// close the connection here
	return mc.conn.Close()

}

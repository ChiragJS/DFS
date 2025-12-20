package uploader

import (
	"dfs/dfs/chunkpb"
	"dfs/internal/client/chunkclient"
	"dfs/internal/client/masterclient"
	"fmt"
)

// provides apis to upload the chunk to chunk server
// should it just accept the string ??
var CHUNK_SIZE int64 = 64 * 1024 * 1024
var SEND_CHUNK_SIZE int64 = 64 * 1024

type Uploader struct {
	chunkServerConn map[string]*chunkclient.ChunkClient
}

// get the master conn here as input ??
func (uc *Uploader) Upload(fileName, path string, masterClient *masterclient.MasterClient) error {
	// everytime a upload is called a new master connection is made ?? or should the
	chunkerOb, err := NewChunker(path, CHUNK_SIZE)

	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer chunkerOb.Close()
	data := make([]byte, CHUNK_SIZE)
	for {
		buff, idx, err := chunkerOb.NextChunk(data)
		// err can also be eof
		if err != nil {
			fmt.Println(err)
			break
		}
		resp, err := masterClient.AllocateChunk(fileName, int32(idx))
		if err != nil {
			return fmt.Errorf("failed to allocate chunk %d for file %s: %w", idx, fileName, err)
		}
		chunkID := resp.GetChunkId()
		replicaServers := resp.GetReplicaServers()

		// master sends alive servers so assume it works
		uploaded := false
		for _, replica := range replicaServers {
			if _, ok := uc.chunkServerConn[replica]; !ok {
				uc.chunkServerConn[replica], err = chunkclient.NewChunkClient(replica)
				if err != nil {
					fmt.Printf("Failed to connect to %s: %v, trying next replica\n", replica, err)
					continue
				}
			}

			stream, err := uc.chunkServerConn[replica].UploadChunk(chunkID)
			if err != nil {
				fmt.Printf("Failed to start upload of chunk %s to %s: %v\n", chunkID, replica, err)
				continue
			}
			// 64 kb is sent at once
			var seqNo int32 = 0
			sendSuccess := true
			for offset := 0; offset < len(buff); offset += int(SEND_CHUNK_SIZE) {
				end := offset + int(SEND_CHUNK_SIZE)
				if end > len(buff) {
					end = len(buff)
				}
				if err := stream.Send(&chunkpb.ChunkData{
					ChunkId:  chunkID,
					Data:     buff[offset:end],
					SeqNo:    seqNo,
					Checksum: 0, // zero for now
				}); err != nil {
					fmt.Printf("Failed to send chunk %s data to %s: %v\n", chunkID, replica, err)
					sendSuccess = false
					break
				}
				seqNo++
			}

			if !sendSuccess {
				stream.CloseAndRecv() // Cleanup stream, ignore error
				continue
			}

			resp, err := stream.CloseAndRecv()
			if err != nil {
				fmt.Printf("Upload of chunk %s to %s failed: %v\n", chunkID, replica, err)
				continue
			}

			if !resp.GetSuccess() {
				fmt.Printf("Upload of chunk %s to %s rejected by server\n", chunkID, replica)
				continue
			}
			fmt.Printf("✓ Upload of chunk %s to %s successful\n", chunkID, replica)
			uploaded = true
			break

		}

		if !uploaded {
			return fmt.Errorf("failed to upload chunk %s to any replica", chunkID)
		}
	}
	return nil
}

func NewUploader() *Uploader {
	return &Uploader{
		chunkServerConn: make(map[string]*chunkclient.ChunkClient),
	}
}

func (uc *Uploader) Close() {
	for k, client := range uc.chunkServerConn {
		client.Close()
		delete(uc.chunkServerConn, k)
	}
}

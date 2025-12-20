package uploader

import (
	"dfs/dfs/chunkpb"
	"dfs/internal/client/chunkclient"
	"dfs/internal/client/masterclient"
	"dfs/pkg/logger"
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
			logger.Debug("Finished reading file", "reason", err)
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
					logger.Warn("Connection failed, trying next replica", "replica", replica, "error", err)
					continue
				}
			}

			stream, err := uc.chunkServerConn[replica].UploadChunk(chunkID)
			if err != nil {
				logger.Warn("Upload start failed", "chunk", chunkID, "replica", replica, "error", err)
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
					logger.Warn("Send failed", "chunk", chunkID, "replica", replica, "error", err)
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
				logger.Warn("Upload failed", "chunk", chunkID, "replica", replica, "error", err)
				continue
			}

			if !resp.GetSuccess() {
				logger.Warn("Upload rejected by server", "chunk", chunkID, "replica", replica)
				continue
			}
			logger.Info("Chunk uploaded", "chunk", chunkID, "replica", replica)
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

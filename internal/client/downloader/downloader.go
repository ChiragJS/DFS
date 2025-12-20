package downloader

import (
	"dfs/internal/client/chunkclient"
	"dfs/internal/client/masterclient"
	"dfs/pkg/logger"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var CHUNK_SIZE = 64 * 1024 * 1024

type Downloader struct {
	chunkClients map[string]*chunkclient.ChunkClient
}

// will contain something , idk rn

// check about the meta data from the master , then try download
// return error instead of printing for now
func (dc *Downloader) Download(fileName string, masterClient *masterclient.MasterClient, downloadPath string) error {
	resp, err := masterClient.GetFileInfo(fileName)
	if err != nil {
		return fmt.Errorf("failed to get file metadata for %s: %w", fileName, err)
	}

	// traverse over the file info and download chunks
	fileInfo := resp.GetFileInfo()
	// sort it!!
	sort.Slice(fileInfo, func(i, j int) bool { //chunkID = fileName_idx
		cid1 := fileInfo[i].ChunkId
		cid2 := fileInfo[j].ChunkId

		parts1 := strings.Split(cid1, "_")
		parts2 := strings.Split(cid2, "_")

		// Safety check: ensure we have at least 2 parts
		if len(parts1) < 2 || len(parts2) < 2 {
			return cid1 < cid2 // Fallback to string comparison
		}

		// Parse indices from last part
		idx1, err1 := strconv.Atoi(parts1[len(parts1)-1])
		idx2, err2 := strconv.Atoi(parts2[len(parts2)-1])

		if err1 != nil || err2 != nil {
			return cid1 < cid2 // Fallback to string comparison
		}

		return idx1 < idx2
	})

	if err := os.MkdirAll(downloadPath, 0755); err != nil {
		return fmt.Errorf("failed to create download directory %s: %w", downloadPath, err)
	}
	// should be sorted according to smth
	for _, chunk := range fileInfo {
		// download the chunk
		chunkId := chunk.GetChunkId()
		replicaServers := chunk.GetReplicaServers()

		// try to download from the replica servers
		for _, replicaServer := range replicaServers {
			// create a connection
			if _, ok := dc.chunkClients[replicaServer]; !ok {
				chunkClient, err := chunkclient.NewChunkClient(replicaServer)
				if err != nil {
					logger.Warn("Connection failed, trying next replica", "replica", replicaServer, "error", err)
					continue // this means connection couldn't be formed so need to get data from other server
				}
				dc.chunkClients[replicaServer] = chunkClient
			}

			//download the chunk

			chunkClient := dc.chunkClients[replicaServer]
			stream, err := chunkClient.DownloadChunk(chunkId)
			if err != nil {
				logger.Warn("Download start failed", "chunk", chunkId, "replica", replicaServer, "error", err)
				continue
			}

			// now download from the stream
			buff := make([]byte, 0, CHUNK_SIZE)
			var success bool = false
			for {
				chunkData, err := stream.Recv()
				if err == io.EOF {
					success = true
					break
				}
				if err != nil {
					logger.Warn("Stream error", "chunk", chunkId, "error", err)
					break
				}
				buff = append(buff, chunkData.Data...)
			}
			// err maybe not useful here
			if !success {
				logger.Warn("Download failed, trying next replica", "chunk", chunkId, "replica", replicaServer)
				continue
			}

			// open a file in append mode
			if err := dc.writeChunkToFile(buff, filepath.Join(downloadPath, fileName)); err != nil {
				logger.Warn("Write failed, trying next replica", "chunk", chunkId, "error", err)
				continue
			}
			// check if the file is downloaded

			break

		}

	}
	return nil
}
func NewDownloader() *Downloader {
	return &Downloader{
		chunkClients: make(map[string]*chunkclient.ChunkClient),
	}
}
func (dc *Downloader) Close() {
	for k, chunkClient := range dc.chunkClients {
		chunkClient.Close()
		delete(dc.chunkClients, k)
	}
}
func (dc *Downloader) writeChunkToFile(buff []byte, filePath string) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(buff); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

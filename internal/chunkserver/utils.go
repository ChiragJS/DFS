package chunkserver

import (
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func disk_usage() (int64, error) {
	var stats syscall.Statfs_t
	err := syscall.Statfs("/home", &stats)
	if err != nil {
		return 0, err
	}
	availableBlocks := stats.Bavail
	blockSize := stats.Bsize
	storageAvailable := (int64(availableBlocks) * blockSize) / (1024 * 1024)
	return int64(storageAvailable), nil
}

func getConnection(address string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

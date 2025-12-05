package chunkserver

import "syscall"

func disk_usage() (int64, error) {
	var stats syscall.Statfs_t
	err := syscall.Statfs("/home", &stats)
	if err != nil {
		return 0, err
	}
	availableBlocks := stats.Bavail
	blockSize := stats.Bfree
	storageAvailable := (availableBlocks * blockSize) / (1024 * 1024)
	return int64(storageAvailable), nil
}

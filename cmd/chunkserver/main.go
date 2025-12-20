package main

import (
	"dfs/internal/chunkserver"
	"dfs/pkg/logger"
	"flag"
)

func main() {
	var (
		port       string
		masterAddr string
		dir        string
	)

	flag.StringVar(&port, "port", ":9001", "chunkserver address")
	flag.StringVar(&masterAddr, "master", ":8000", "master address")
	flag.StringVar(&dir, "dir", "./data", "storage directory")
	flag.Parse()

	logger.Info("Starting chunk server", "port", port, "master", masterAddr, "dir", dir)

	cs := chunkserver.NewChunkServer(port, masterAddr, dir)

	if err := cs.Start(); err != nil {
		logger.Error("Chunk server failed", "error", err)
	}
}

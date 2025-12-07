package main

import (
	"dfs/internal/chunkserver"
	"flag"
	"fmt"
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

	fmt.Println("Starting chunkserver on", port)

	cs := chunkserver.NewChunkServer(port, masterAddr, dir)

	if err := cs.Start(); err != nil {
		panic(err)
	}

}

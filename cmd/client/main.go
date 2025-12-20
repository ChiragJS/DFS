package main

import (
	"dfs/internal/client"
	"dfs/pkg/logger"
	"flag"
	"fmt"
	"os"
)

func main() {
	// Define flags
	masterAddr := flag.String("master", "localhost:8000", "Master server address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]

	// Create DFS client
	dfsClient, err := client.NewDFSClient(*masterAddr)
	if err != nil {
		logger.Error("Failed to connect to DFS", "error", err)
		os.Exit(1)
	}
	defer dfsClient.Close()

	switch command {
	case "put":
		if len(args) < 3 {
			fmt.Println("Usage: client put <local-file> <remote-name>")
			os.Exit(1)
		}
		localPath := args[1]
		remoteName := args[2]

		if err := dfsClient.Put(remoteName, localPath); err != nil {
			logger.Error("Upload failed", "error", err)
			os.Exit(1)
		}

	case "get":
		if len(args) < 3 {
			fmt.Println("Usage: client get <remote-name> <local-dir>")
			os.Exit(1)
		}
		remoteName := args[1]
		localDir := args[2]

		if err := dfsClient.Get(remoteName, localDir); err != nil {
			logger.Error("Download failed", "error", err)
			os.Exit(1)
		}

	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("DFS Client")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  client [flags] <command> [arguments]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  put <local-file> <remote-name>  Upload a file to DFS")
	fmt.Println("  get <remote-name> <local-dir>   Download a file from DFS")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("  -master string   Master server address (default \"localhost:8000\")")
}

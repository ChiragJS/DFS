package main

import (
	"dfs/internal/master"
	"dfs/pkg/logger"
)

func main() {
	m := master.NewMasterServer()
	if err := m.Start(); err != nil {
		logger.Error("Master server failed", "error", err)
	}
}

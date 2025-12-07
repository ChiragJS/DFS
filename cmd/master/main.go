package main

import (
	"dfs/internal/master"
	"fmt"
)

func main() {
	master := master.NewMasterServer()
	err := master.Start()
	if err != nil {
		fmt.Print(err)
	}
}

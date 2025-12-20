package main

import (
	"bytes"
	"dfs/internal/client/uploader"
	"fmt"
	"io"
)

func main() {

	chunker, err := uploader.NewChunker("./output.txt", int64(16))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 16)
	for {
		data, _, err := chunker.NextChunk(buff)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(bytes.NewBuffer(data).String())
		fmt.Println("*****************")
	}
	fmt.Println("Seek")
	data, _ := chunker.GetChunkAtIndex(0, buff)
	fmt.Println(bytes.NewBuffer(data).String())

	fmt.Println("Next")
	data, _, _ = chunker.NextChunk(buff)
	fmt.Println(bytes.NewBuffer(data).String())
	chunker.Close()
}

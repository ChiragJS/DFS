package uploader

import (
	"io"
	"os"
)

// chunker should --> open the file read 64 mb me break it --> now with that 64 mb , break it into 64 kb blocks and send it
// should i have a struct ?? if i get a file
type Chunker struct {
	file       *os.File
	chunkSize  int64
	chunkCount int
	// add more when needed
}

func NewChunker(filePath string, chunkSize int64) (*Chunker, error) {

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
		// handle it
	}
	return &Chunker{
		file:       file,
		chunkSize:  chunkSize,
		chunkCount: 0,
	}, nil

}

// add utility functions for reading and stuff

func (chunker *Chunker) NextChunk(buff []byte) ([]byte, int, error) {

	// is it possible ? at a time i am reading

	n, err := chunker.file.Read(buff)
	if err != nil {
		return nil, 0, err
		// handle it do something about it for now
	}
	chunker.chunkCount++
	return buff[:n], chunker.chunkCount, nil
}

func (chunker *Chunker) GetChunkAtIndex(index int, buff []byte) ([]byte, error) {
	// buff := make([]byte , chunker.chunkSize)
	offset := int64(index) * (chunker.chunkSize)
	_, err := chunker.file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	n, err := chunker.file.Read(buff)
	if err != nil {
		return nil, err
	}
	_, err = chunker.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	// sets it back to last position!!
	return buff[:n], err

}

func (chunker *Chunker) Close() {
	chunker.file.Close()
}

// once i change the offset , will it point to the last executed one ??
// be wary of the offset implementation , read more about the seek function in detail
// test this function out first

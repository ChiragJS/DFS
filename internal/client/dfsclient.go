package client

import (
	"dfs/internal/client/downloader"
	"dfs/internal/client/masterclient"
	"dfs/internal/client/uploader"
	"fmt"
)

type DFSClient struct {
	masterClient *masterclient.MasterClient
	uploader     *uploader.Uploader
	downloader   *downloader.Downloader
}

func NewDFSClient(masterAddress string) (*DFSClient, error) {
	masterClient, err := masterclient.NewMasterClient(masterAddress)
	if err != nil {
		return nil, err
	}
	return &DFSClient{
		masterClient: masterClient,
		uploader:     uploader.NewUploader(),
		downloader:   downloader.NewDownloader(),
	}, nil
}

func (dfsClient *DFSClient) Close() {
	dfsClient.masterClient.Close()
	dfsClient.uploader.Close()
	dfsClient.downloader.Close()
}

func (dfsClient *DFSClient) Put(fileName, path string) error {
	fmt.Printf("Uploading %s to DFS as %s...\n", path, fileName)

	err := dfsClient.uploader.Upload(fileName, path, dfsClient.masterClient)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	fmt.Printf("Successfully uploaded %s\n", fileName)
	return nil
}

func (dfsClient *DFSClient) Get(fileName, downloadPath string) error {
	fmt.Printf("Downloading %s from DFS to %s...\n", fileName, downloadPath)

	err := dfsClient.downloader.Download(fileName, dfsClient.masterClient, downloadPath)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	fmt.Printf("Successfully downloaded to %s/%s\n", downloadPath, fileName)
	return nil
}

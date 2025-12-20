package client

import (
	"dfs/internal/client/downloader"
	"dfs/internal/client/masterclient"
	"dfs/internal/client/uploader"
	"dfs/pkg/logger"
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
	logger.Info("Starting upload", "file", fileName, "path", path)

	err := dfsClient.uploader.Upload(fileName, path, dfsClient.masterClient)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	logger.Info("Upload successful", "file", fileName)
	return nil
}

func (dfsClient *DFSClient) Get(fileName, downloadPath string) error {
	logger.Info("Starting download", "file", fileName, "path", downloadPath)

	err := dfsClient.downloader.Download(fileName, dfsClient.masterClient, downloadPath)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	logger.Info("Download successful", "file", fileName, "path", downloadPath)
	return nil
}

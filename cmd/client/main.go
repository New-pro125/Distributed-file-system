package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/New-pro125/distributed-file-system/client"
)

func main() {
	// Command-line flags
	masterAddr := flag.String("master", "localhost:50051", "Address of the Master Tracker")
	command := flag.String("cmd", "", "Command to execute: upload or download")
	filePath := flag.String("file", "", "Path to the file (for upload) or save path (for download)")
	fileName := flag.String("name", "", "Name of the file (for download)")
	parallel := flag.Bool("parallel", false, "Use parallel download (bonus feature)")

	flag.Parse()

	if *command == "" {
		fmt.Println("Usage:")
		fmt.Println("  Upload:   -cmd=upload -file=<path-to-file>")
		fmt.Println("  Download: -cmd=download -name=<filename> -file=<save-path> [-parallel]")
		os.Exit(1)
	}

	// Create client
	c, err := client.New(*masterAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch *command {
	case "upload":
		if *filePath == "" {
			log.Fatal("Please specify a file path with -file")
		}

		log.Printf("Uploading file: %s", *filePath)
		if err := c.Upload(ctx, *filePath); err != nil {
			log.Fatalf("Upload failed: %v", err)
		}
		fmt.Println("Upload completed successfully!")

	case "download":
		if *fileName == "" || *filePath == "" {
			log.Fatal("Please specify both -name and -file for download")
		}

		log.Printf("Downloading file: %s", *fileName)

		if *parallel {
			log.Println("Using parallel download mode")
			if err := c.DownloadParallel(ctx, *fileName, *filePath); err != nil {
				log.Fatalf("Parallel download failed: %v", err)
			}
		} else {
			if err := c.Download(ctx, *fileName, *filePath); err != nil {
				log.Fatalf("Download failed: %v", err)
			}
		}

		fmt.Printf("Download completed successfully! File saved to: %s\n", *filePath)

	default:
		log.Fatalf("Unknown command: %s (use 'upload' or 'download')", *command)
	}
}

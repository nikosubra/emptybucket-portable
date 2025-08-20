package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
	"golang.org/x/sync/semaphore"

	"github.com/nikosubra/emptybucket-portable/deleter"
	"github.com/nikosubra/emptybucket-portable/lister"
	"github.com/nikosubra/emptybucket-portable/logger"
	"github.com/nikosubra/emptybucket-portable/state"
)

// Initialize the S3 client with provided credentials, region and custom endpoint.
// Disable TLS verification to allow connections to endpoints with invalid certificates.
func initS3Client(accessKey, secretKey, region, endpoint string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(
			aws.NewCredentialsCache(
				credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			),
		),
		config.WithEndpointResolver(
			aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			}),
		),
		config.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}),
		config.WithLogger(logging.NewStandardLogger(os.Stderr)),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func main() {

	timeoutHours := flag.Int("timeout", 36, "Global timeout for execution in hours")
	workers := flag.Int("workers", 4, "Number of concurrent deletion workers")
	batch := flag.Int("batch-size", 200, "Number of objects to delete per batch")
	estimateETA := flag.Bool("estimate-eta", false, "Enable approximate ETA based on batch processing rate")
	dryRun := flag.Bool("dry-run", false, "Simulate deletions without actually deleting objects")
	logLevel := flag.String("log-level", "info", "Set log level: debug, info, warn, error")

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutHours)*time.Hour)
	defer cancel()

	// Handle interrupt signals to gracefully shut down
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		logger.Warn("Interrupt signal received. Cancelling context...")
		cancel()
	}()

	resetState := flag.Bool("reset-state", false, "Ignore and delete existing state.json")
	flag.Parse()

	logger.SetLevel(*logLevel)

	// Initial message and store start time of execution
	fmt.Println("Starting bucket cleanup script...")
	start := time.Now()

	// Collect user input: AWS credentials, bucket, endpoint and region
	var accessKey, secretKey, bucket, endpoint, region string
	fmt.Print("Enter AWS Access Key: ")
	fmt.Scanln(&accessKey)
	fmt.Print("Enter AWS Secret Key: ")
	fmt.Scanln(&secretKey)
	fmt.Print("Enter Bucket name: ")
	fmt.Scanln(&bucket)
	fmt.Print("Enter S3 Endpoint: ")
	fmt.Scanln(&endpoint)
	fmt.Print("Enter Region: ")
	fmt.Scanln(&region)

	// Setup logging: create log file and write error logs only to file
	logFile, err := os.Create("output.log")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()

	logger.Init(logFile)

	// Create JSON log file with error handling for opening
	jsonLogFile, err := os.Create("log_json.json")
	if err != nil {
		logger.Warn("Error opening log_json file: %v\n", err)
	}
	defer jsonLogFile.Close()

	// Load state from state.json if exists
	stateFileName := "state.json"

	processed, err := state.LoadState(stateFileName, *resetState)
	if err != nil {
		logger.Warn("State load error: %v", err)
	}

	// Initialize S3 client with provided credentials and configurations
	client, err := initS3Client(accessKey, secretKey, region, endpoint)
	if err != nil {
		logger.Error("Config error: %v\n", err)
		log.Fatalf("Config error: %v\n", err)
	}

	// Check accessibility of the specified bucket via HeadBucket
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		logger.Error("Bucket not accessible: %v\n", err)
		log.Fatalf("Bucket not accessible: %v\n", err)
	}

	// Retrieve and log bucket versioning status in JSON format
	verCfg, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	versioningEnabled := verCfg.Status == "Enabled"
	if err == nil {
		json.NewEncoder(jsonLogFile).Encode(map[string]interface{}{
			"time":             time.Now().Format(time.RFC3339),
			"bucket":           bucket,
			"region":           region,
			"endpoint":         endpoint,
			"versioningStatus": string(verCfg.Status),
			"mfaDelete":        string(verCfg.MFADelete),
		})
	}

	// Settings for batch deletion with concurrency limited by semaphore
	batchSize := *batch
	numWorkers := *workers
	sem := semaphore.NewWeighted(int64(numWorkers))
	var wg sync.WaitGroup
	var mu sync.Mutex

	var failedObjects []types.ObjectIdentifier

	batchChan := make(chan []types.ObjectIdentifier, 10)
	lister.StartProducer(ctx, client, bucket, processed, batchSize, batchChan, logger.Info, logger.Error, versioningEnabled)

	// Use deleter package to start worker pool and process batches
	delResult := deleter.StartWorkerPool(ctx, deleter.DeleterConfig{
		Client:      client,
		Bucket:      bucket,
		BatchSize:   batchSize,
		EstimateETA: *estimateETA,
		LogFile:     logFile,
		Processed:   processed,
		StateLock:   &mu,
		Semaphore:   sem,
		StartTime:   start,
		DryRun:      *dryRun,
	}, batchChan, &wg, logger.Info, logger.Warn, logger.Error)

	deletedCount := delResult.DeletedCount
	errorCount := delResult.ErrorCount
	failedObjects = delResult.FailedObjects

	logger.Info("All deletion workers completed.")

	// Print final summary of deletion operations
	fmt.Printf("\nDeleted: %d | Errors: %d\n", deletedCount, errorCount)

	// Write to CSV file the objects that were not successfully deleted
	if len(failedObjects) > 0 {
		fcsv, _ := os.Create("failures.csv")
		writer := csv.NewWriter(fcsv)
		defer fcsv.Close()
		writer.Write([]string{"Key", "VersionId"})
		for _, obj := range failedObjects {
			writer.Write([]string{aws.ToString(obj.Key), aws.ToString(obj.VersionId)})
		}
		writer.Flush()
		fmt.Printf("Wrote %d failed deletions to failures.csv\n", len(failedObjects))
	}

	// Save updated state to state.json
	if err := state.SaveState(stateFileName, processed); err != nil {
		logger.Warn("Failed to write state.json: %v", err)
	}

	// Print total time taken to complete the script
	duration := time.Since(start)
	fmt.Printf("Completed in: %v\n", duration)

	logger.Info("Final summary: Deleted %d objects, Encountered %d errors", deletedCount, errorCount)
	logger.Info("Total duration: %s", duration.Truncate(time.Second))
	logger.Info("Failed deletions written to failures.csv: %d", len(failedObjects))

	fmt.Println("\nℹ️  No objects found for deletion in the bucket.")
	fmt.Printf("⏱️  Total duration: %s\n", duration.Truncate(time.Second))
	fmt.Printf("✅ Deleted: %d\n", deletedCount)
	fmt.Printf("❌ Errors: %d\n", errorCount)
	fmt.Printf("📄 Failures logged in: failures.csv\n")

	metrics := map[string]interface{}{
		"duration":       duration.Truncate(time.Second).String(),
		"deleted":        deletedCount,
		"errors":         errorCount,
		"failuresLogged": len(failedObjects),
		"totalObjects":   0,
		"timestamp":      time.Now().Format(time.RFC3339),
	}
	metricsFile, err := os.Create("metrics.json")
	if err != nil {
		logger.Warn("Failed to create metrics.json: %v", err)
	} else {
		encoder := json.NewEncoder(metricsFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(metrics); err != nil {
			logger.Warn("Failed to write metrics.json: %v", err)
		}
		metricsFile.Close()
	}
}

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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/semaphore"
)

// Custom logging functions for different log levels: info, warning and errors
func logInfo(format string, v ...interface{}) {
	log.Printf("[INFO] "+format, v...)
}

func logWarn(format string, v ...interface{}) {
	log.Printf("[WARN] "+format, v...)
}

var errorLog *log.Logger

func logError(format string, v ...interface{}) {
	msg := fmt.Sprintf("[ERROR] "+format, v...)
	fmt.Println(msg)
	errorLog.Println(msg)
}

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

	resetState := flag.Bool("reset-state", false, "Ignore and delete existing state.json")
	flag.Parse()

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

	errorLog = log.New(logFile, "", log.LstdFlags)

	// Create JSON log file with error handling for opening
	jsonLogFile, err := os.Create("log_json.json")
	if err != nil {
		logWarn("Error opening log_json file: %v\n", err)
	}
	defer jsonLogFile.Close()

	// Load state from state.json if exists
	processed := make(map[string]map[string]bool)
	stateFileName := "state.json"

	if *resetState {
		if err := os.Remove(stateFileName); err == nil {
			logInfo("Previous state.json deleted due to --reset-state flag.")
		} else if !os.IsNotExist(err) {
			logWarn("Could not delete state.json: %v", err)
		}
	} else {
		stateFile, err := os.Open(stateFileName)
		if err == nil {
			defer stateFile.Close()
			var stateMap map[string][]string
			decoder := json.NewDecoder(stateFile)
			if err := decoder.Decode(&stateMap); err == nil {
				for key, versions := range stateMap {
					if processed[key] == nil {
						processed[key] = make(map[string]bool)
					}
					for _, v := range versions {
						processed[key][v] = true
					}
				}
				logInfo("Loaded state from %s", stateFileName)
			} else {
				logWarn("Failed to decode state.json: %v", err)
			}
		} else if !os.IsNotExist(err) {
			logWarn("Error opening state.json: %v", err)
		}
	}

	// Initialize S3 client with provided credentials and configurations
	client, err := initS3Client(accessKey, secretKey, region, endpoint)
	if err != nil {
		logError("Config error: %v\n", err)
		log.Fatalf("Config error: %v\n", err)
	}

	// Check accessibility of the specified bucket via HeadBucket
	_, err = client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		logError("Bucket not accessible: %v\n", err)
		log.Fatalf("Bucket not accessible: %v\n", err)
	}

	// Retrieve and log bucket versioning status in JSON format
	verCfg, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
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

	// Scan and count total objects and versions in the bucket
	var totalObjects int
	countPaginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	for countPaginator.HasMorePages() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		page, err := countPaginator.NextPage(ctx)
		cancel()
		if err != nil {
			logError("Error counting objects: %v\n", err)
			log.Fatalf("Error counting objects: %v\n", err)
		}
		for _, v := range page.Versions {
			key := aws.ToString(v.Key)
			if processed[key] != nil && processed[key][aws.ToString(v.VersionId)] {
				continue
			}
			totalObjects++
		}
		for _, dm := range page.DeleteMarkers {
			key := aws.ToString(dm.Key)
			if processed[key] != nil && processed[key][aws.ToString(dm.VersionId)] {
				continue
			}
			totalObjects++
		}
		logInfo("Scanning in progress... %d objects found so far", totalObjects)
	}
	fmt.Printf("Total objects: %d\n", totalObjects)

	// Configure progress bar for deleting objects
	bar := progressbar.NewOptions(totalObjects,
		progressbar.OptionSetDescription("Deleting objects"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(30),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)

	// Settings for batch deletion with concurrency limited by semaphore
	const batchSize = 200
	numWorkers := 4
	sem := semaphore.NewWeighted(int64(numWorkers))
	var wg sync.WaitGroup
	var mu sync.Mutex

	var deletedCount, errorCount int
	var failedObjects []types.ObjectIdentifier

	var batchCount int
	var etaEstimate time.Duration

	paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})

	// Function to process a batch of objects to delete concurrently
	processBatch := func(batch []types.ObjectIdentifier) {
		if len(batch) == 0 {
			return
		}
		wg.Add(1)
		if err := sem.Acquire(context.Background(), 1); err != nil {
			logWarn("Semaphore acquire failed: %v", err)
			wg.Done()
			return
		}
		go func(batch []types.ObjectIdentifier) {
			defer sem.Release(1)
			defer wg.Done()

			input := &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{
					Objects: batch,
				},
				BypassGovernanceRetention: aws.Bool(true),
			}

			var err error
			var resp *s3.DeleteObjectsOutput
			// Retry attempts in case of deletion error
			for i := 0; i < 3; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				resp, err = client.DeleteObjects(ctx, input)
				cancel()
				if err == nil {
					break
				}
				logWarn("Retry %d for batch: %v\n", i+1, err)
				time.Sleep(time.Second * time.Duration(i+1))
			}

			mu.Lock()
			if err != nil {
				errorCount += len(batch)
				failedObjects = append(failedObjects, batch...)
			} else {
				deletedCount += len(resp.Deleted)
				errorCount += len(resp.Errors)
				for _, e := range resp.Errors {
					failedObjects = append(failedObjects, types.ObjectIdentifier{
						Key:       e.Key,
						VersionId: e.VersionId,
					})
				}
				// Update processed map for successfully deleted objects
				for _, obj := range resp.Deleted {
					key := aws.ToString(obj.Key)
					verId := aws.ToString(obj.VersionId)
					if processed[key] == nil {
						processed[key] = make(map[string]bool)
					}
					processed[key][verId] = true
				}
			}

			// Update progress bar with number of processed objects
			_ = bar.Add(len(batch))

			// Calculate and log estimated ETA based on current progress
			batchCount++
			elapsed := time.Since(start)
			if deletedCount > 0 && totalObjects > deletedCount {
				rate := float64(elapsed.Milliseconds()) / float64(deletedCount)
				etaEstimate = time.Duration(rate*float64(totalObjects-deletedCount)) * time.Millisecond
			}
			logInfo("Batch %d completed. Totals: deleted %d, errors %d, estimated ETA %s", batchCount, deletedCount, errorCount, etaEstimate.Truncate(time.Second))
			mu.Unlock()
		}(batch)
	}

	// Iterate over pages of objects/versions to create batches for deletion
	var currentBatch []types.ObjectIdentifier
	for paginator.HasMorePages() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		page, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			logError("Error listing objects: %v", err)
			log.Fatalf("Error listing objects: %v", err)
		}
		logInfo("S3 page read completed: %d versions and %d delete markers", len(page.Versions), len(page.DeleteMarkers))
		for _, v := range page.Versions {
			key := aws.ToString(v.Key)
			if processed[key] != nil && processed[key][aws.ToString(v.VersionId)] {
				continue
			}
			currentBatch = append(currentBatch, types.ObjectIdentifier{
				Key:       v.Key,
				VersionId: v.VersionId,
			})
			if len(currentBatch) >= batchSize {
				processBatch(currentBatch)
				currentBatch = nil
			}
		}
		for _, dm := range page.DeleteMarkers {
			key := aws.ToString(dm.Key)
			if processed[key] != nil && processed[key][aws.ToString(dm.VersionId)] {
				continue
			}
			currentBatch = append(currentBatch, types.ObjectIdentifier{
				Key:       dm.Key,
				VersionId: dm.VersionId,
			})
			if len(currentBatch) >= batchSize {
				processBatch(currentBatch)
				currentBatch = nil
			}
		}
	}
	// Process any remaining objects in the current batch
	if len(currentBatch) > 0 {
		processBatch(currentBatch)
	}

	// Wait for all deletion goroutines to complete
	wg.Wait()

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
	stateMap := make(map[string][]string)
	for key, versionsMap := range processed {
		for ver := range versionsMap {
			stateMap[key] = append(stateMap[key], ver)
		}
	}
	stateFileOut, err := os.Create(stateFileName)
	if err != nil {
		logWarn("Failed to create state.json: %v", err)
	} else {
		encoder := json.NewEncoder(stateFileOut)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(stateMap); err != nil {
			logWarn("Failed to write state.json: %v", err)
		}
		stateFileOut.Close()
	}

	// Print total time taken to complete the script
	duration := time.Since(start)
	fmt.Printf("Completed in: %v\n", duration)
}

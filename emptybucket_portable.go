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

	timeoutHours := flag.Int("timeout", 36, "Global timeout for execution in hours")
	workers := flag.Int("workers", 4, "Number of concurrent deletion workers")
	batch := flag.Int("batch-size", 200, "Number of objects to delete per batch")

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutHours)*time.Hour)
	defer cancel()

	// Handle interrupt signals to gracefully shut down
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		logWarn("Interrupt signal received. Cancelling context...")
		cancel()
	}()

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
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
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

	// Use an indeterminate progress bar for scanning (no pre-scan for estimated total).

	// Scan and count total objects and versions in the bucket
	var totalObjects int
	countPaginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})

	scanBar := progressbar.NewOptions(-1,
		progressbar.OptionSetDescription("Scanning bucket"),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionSetWidth(30),
		progressbar.OptionSetRenderBlankState(true),
	)

	// Track scan start time
	scanStart := time.Now()

	for countPaginator.HasMorePages() {
		ctxPage, cancel := context.WithTimeout(ctx, 60*time.Second)
		page, err := countPaginator.NextPage(ctxPage)
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
			_ = scanBar.Add(1)
		}
		for _, dm := range page.DeleteMarkers {
			key := aws.ToString(dm.Key)
			if processed[key] != nil && processed[key][aws.ToString(dm.VersionId)] {
				continue
			}
			totalObjects++
			_ = scanBar.Add(1)
		}
	}
	scanDuration := time.Since(scanStart)
	fmt.Printf("Total objects: %d\n", totalObjects)
	fmt.Printf("⏱️  Scanning completed in: %s\n", scanDuration.Truncate(time.Second))

	// Configure progress bar for deleting objects
	delBar := progressbar.NewOptions(totalObjects,
		progressbar.OptionSetDescription("Deleting objects"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionSetRenderBlankState(true),
	)

	// Settings for batch deletion with concurrency limited by semaphore
	batchSize := *batch
	numWorkers := *workers
	sem := semaphore.NewWeighted(int64(numWorkers))
	var wg sync.WaitGroup
	var mu sync.Mutex

	var deletedCount, errorCount int
	var failedObjects []types.ObjectIdentifier

	var batchCount int
	var etaEstimate time.Duration

	var consecutiveErrors int
	const maxConsecutiveErrors = 5

	paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})

	batchChan := make(chan []types.ObjectIdentifier, 10)

	// Function to process a batch of objects to delete concurrently
	processBatch := func(batch []types.ObjectIdentifier) {
		if len(batch) == 0 {
			return
		}
		wg.Add(1)
		if err := sem.Acquire(ctx, 1); err != nil {
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
				ctxDel, cancel := context.WithTimeout(ctx, 60*time.Second)
				resp, err = client.DeleteObjects(ctxDel, input)
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
			_ = delBar.Add(len(batch))

			// Calculate and log estimated ETA based on current progress
			batchCount++
			elapsed := time.Since(start)
			if deletedCount > 0 && totalObjects > deletedCount {
				rate := float64(elapsed.Milliseconds()) / float64(deletedCount)
				etaEstimate = time.Duration(rate*float64(totalObjects-deletedCount)) * time.Millisecond
			}
			logInfo("Batch %d completed. Totals: deleted %d, errors %d, estimated ETA %s", batchCount, deletedCount, errorCount, etaEstimate.Truncate(time.Second))
			logFile.Sync()

			if err != nil || (resp != nil && len(resp.Errors) > 0) {
				consecutiveErrors++
			} else {
				consecutiveErrors = 0
			}

			if consecutiveErrors >= maxConsecutiveErrors {
				sleepDuration := time.Duration(consecutiveErrors) * time.Second
				logWarn("High error rate detected. Throttling by sleeping %v...", sleepDuration)
				mu.Unlock() // release lock before sleep
				time.Sleep(sleepDuration)
				mu.Lock()
				consecutiveErrors = 0 // reset after throttling
			}

			mu.Unlock()
		}(batch)
	}

	// Producer goroutine to read pages and send batches to batchChan
	go func() {
		defer close(batchChan)
		var currentBatch []types.ObjectIdentifier
		for paginator.HasMorePages() {
			ctxPage, cancel := context.WithTimeout(ctx, 60*time.Second)
			page, err := paginator.NextPage(ctxPage)
			cancel()
			if err != nil {
				logError("Error listing objects: %v", err)
				return
			}
			logInfo("S3 page read completed: %d versions and %d delete markers", len(page.Versions), len(page.DeleteMarkers))

			for _, v := range page.Versions {
				key := aws.ToString(v.Key)
				ver := aws.ToString(v.VersionId)
				if processed[key] != nil && processed[key][ver] {
					continue
				}
				currentBatch = append(currentBatch, types.ObjectIdentifier{
					Key:       v.Key,
					VersionId: v.VersionId,
				})
				if len(currentBatch) >= batchSize {
					batchChan <- currentBatch
					currentBatch = nil
				}
			}
			for _, dm := range page.DeleteMarkers {
				key := aws.ToString(dm.Key)
				ver := aws.ToString(dm.VersionId)
				if processed[key] != nil && processed[key][ver] {
					continue
				}
				currentBatch = append(currentBatch, types.ObjectIdentifier{
					Key:       dm.Key,
					VersionId: dm.VersionId,
				})
				if len(currentBatch) >= batchSize {
					batchChan <- currentBatch
					currentBatch = nil
				}
			}
		}
		if len(currentBatch) > 0 {
			batchChan <- currentBatch
		}
	}()

	// Consumer worker pool to process batches from batchChan
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchChan {
				processBatch(batch)
			}
		}()
	}

	// Wait for all deletion goroutines to complete
	wg.Wait()

	logInfo("All deletion workers completed.")

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

	logInfo("Final summary: Deleted %d objects, Encountered %d errors", deletedCount, errorCount)
	logInfo("Total duration: %s", duration.Truncate(time.Second))
	logInfo("Failed deletions written to failures.csv: %d", len(failedObjects))

	fmt.Println("\n✔️  Cleanup completed successfully.")
	fmt.Printf("⏱️  Total duration: %s\n", duration.Truncate(time.Second))
	fmt.Printf("✅ Deleted: %d\n", deletedCount)
	fmt.Printf("❌ Errors: %d\n", errorCount)
	fmt.Printf("📄 Failures logged in: failures.csv\n")

	metrics := map[string]interface{}{
		"duration":       duration.Truncate(time.Second).String(),
		"deleted":        deletedCount,
		"errors":         errorCount,
		"failuresLogged": len(failedObjects),
		"totalObjects":   totalObjects,
		"timestamp":      time.Now().Format(time.RFC3339),
	}
	metricsFile, err := os.Create("metrics.json")
	if err != nil {
		logWarn("Failed to create metrics.json: %v", err)
	} else {
		encoder := json.NewEncoder(metricsFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(metrics); err != nil {
			logWarn("Failed to write metrics.json: %v", err)
		}
		metricsFile.Close()
	}
}

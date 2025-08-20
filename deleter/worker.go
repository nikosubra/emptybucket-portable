package deleter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/semaphore"
)

type DeleterConfig struct {
	Client      *s3.Client
	Bucket      string
	BatchSize   int
	EstimateETA bool
	LogFile     *os.File
	Processed   map[string]map[string]bool
	StateLock   *sync.Mutex
	Semaphore   *semaphore.Weighted
	StartTime   time.Time
	NumWorkers  int
	DryRun      bool
}

type Result struct {
	DeletedCount  int
	ErrorCount    int
	FailedObjects []types.ObjectIdentifier
}

func StartWorkerPool(
	ctx context.Context,
	cfg DeleterConfig,
	batchChan <-chan []types.ObjectIdentifier,
	wg *sync.WaitGroup,
	logger func(string, ...interface{}),
	warn func(string, ...interface{}),
	errorLogger func(string, ...interface{}),
) Result {

	var deletedCount, errorCount, batchCount, consecutiveErrors int
	const maxConsecutiveErrors = 5
	var failedObjects []types.ObjectIdentifier
	var etaEstimate time.Duration

	processBatch := func(batch []types.ObjectIdentifier) {
		if len(batch) == 0 {
			return
		}
		wg.Add(1)
		if err := cfg.Semaphore.Acquire(ctx, 1); err != nil {
			warn("Semaphore acquire failed: %v", err)
			wg.Done()
			return
		}

		go func(batch []types.ObjectIdentifier) {
			defer cfg.Semaphore.Release(1)
			defer wg.Done()

			input := &s3.DeleteObjectsInput{
				Bucket:                    aws.String(cfg.Bucket),
				Delete:                    &types.Delete{Objects: batch},
				BypassGovernanceRetention: aws.Bool(true),
			}

			var err error
			var resp *s3.DeleteObjectsOutput
			for i := 0; i < 3; i++ {
				ctxDel, cancel := context.WithTimeout(ctx, 60*time.Second)
				resp, err = cfg.Client.DeleteObjects(ctxDel, input)
				cancel()
				if err == nil {
					break
				}
				warn("Retry %d for batch: %v", i+1, err)
				time.Sleep(time.Second * time.Duration(i+1))
			}

			cfg.StateLock.Lock()
			defer cfg.StateLock.Unlock()

			if err != nil {
				errorCount += len(batch)
				failedObjects = append(failedObjects, batch...)
			} else {
				deletedCount += len(resp.Deleted)
				errorCount += len(resp.Errors)
				for _, e := range resp.Errors {
					failedObjects = append(failedObjects, types.ObjectIdentifier{
						Key: e.Key, VersionId: e.VersionId,
					})
				}
				for _, obj := range resp.Deleted {
					key := aws.ToString(obj.Key)
					ver := aws.ToString(obj.VersionId)
					if cfg.Processed[key] == nil {
						cfg.Processed[key] = make(map[string]bool)
					}
					cfg.Processed[key][ver] = true
				}
			}

			fmt.Printf("\r✅ Deleted: %d | ❌ Errors: %d", deletedCount, errorCount)

			batchCount++
			if batchCount%10 == 0 {
				logger("Progress log — %s — Deleted: %d | Errors: %d", time.Now().Format(time.RFC3339), deletedCount, errorCount)
				cfg.LogFile.Sync()
			}

			if cfg.EstimateETA && deletedCount > 0 && batchCount > 0 {
				avgBatchDuration := time.Since(cfg.StartTime) / time.Duration(batchCount)
				remainingObjects := ((deletedCount/cfg.BatchSize)+1)*cfg.BatchSize - deletedCount
				etaEstimate = avgBatchDuration * time.Duration(remainingObjects)
			}

			if cfg.EstimateETA {
				logger("Batch %d completed. Totals: deleted %d, errors %d, estimated ETA %s", batchCount, deletedCount, errorCount, etaEstimate.Truncate(time.Second))
			} else {
				logger("Batch %d completed. Totals: deleted %d, errors %d", batchCount, deletedCount, errorCount)
			}
			cfg.LogFile.Sync()

			if err != nil || (resp != nil && len(resp.Errors) > 0) {
				consecutiveErrors++
			} else {
				consecutiveErrors = 0
			}

			if consecutiveErrors >= maxConsecutiveErrors {
				sleepDuration := time.Duration(consecutiveErrors) * time.Second
				warn("High error rate detected. Throttling by sleeping %v...", sleepDuration)
				time.Sleep(sleepDuration)
				consecutiveErrors = 0
			}
		}(batch)
	}

	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchChan {
				processBatch(batch)
			}
		}()
	}

	wg.Wait()
	return Result{
		DeletedCount:  deletedCount,
		ErrorCount:    errorCount,
		FailedObjects: failedObjects,
	}
}

// Package deleter runs a worker-pool batch DeleteObjects loop against S3.
// It is engine-agnostic of UI: consumers receive structured DeletionEvents
// and atomic live counters; the deleter does not write to stdout.
package deleter

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/semaphore"
)

// DeletionEvent is emitted for every object the deleter processed. Consumers
// (TUI, web UI, CLI) read this to show "what is being deleted" in real time.
// Failed is set when the delete API returned an error for this key.
type DeletionEvent struct {
	Key       string
	VersionId string
	Timestamp time.Time
	Failed    bool
	Err       string
}

// Config holds the runtime parameters for a deletion run. All fields are
// owned by the caller; the deleter does not mutate or close them.
type Config struct {
	Client       *s3.Client
	Bucket       string
	BatchSize    int
	NumWorkers   int
	DryRun       bool
	Retries      int   // 0 → default 3
	TotalObjects int64 // 0 disables ETA-style logging
	StartTime    time.Time

	// EventChan receives one DeletionEvent per processed object. Optional;
	// nil disables emission. Sends are non-blocking so a stalled consumer
	// cannot delay deletions.
	EventChan chan<- DeletionEvent

	// LiveDeleted / LiveErrors, when non-nil, are incremented atomically as
	// objects succeed or fail. Callers use them for live progress widgets
	// independent of EventChan back-pressure.
	LiveDeleted *int64
	LiveErrors  *int64
}

type Result struct {
	DeletedCount  int
	ErrorCount    int
	FailedObjects []types.ObjectIdentifier
}

// throughputSample is one (timestamp, cumulative-deleted) data point used by
// the rolling window to compute objects/sec.
type throughputSample struct {
	T       time.Time
	Deleted int64
}

// Run consumes batches from batchChan and deletes them concurrently. The
// function blocks until batchChan is closed and all in-flight batches have
// completed; on return, EventChan (when non-nil) is closed.
func Run(
	ctx context.Context,
	cfg Config,
	batchChan <-chan []types.ObjectIdentifier,
	logger func(string, ...interface{}),
	warn func(string, ...interface{}),
	errorLogger func(string, ...interface{}),
) Result {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 4
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 3
	}

	var (
		deletedCount, errorCount, batchCount, consecutiveErrors int
		failedObjects                                           []types.ObjectIdentifier
		mu                                                      sync.Mutex
		samples                                                 = make([]throughputSample, 0, 20)
	)
	const maxConsecutiveErrors = 5
	const rollingWindow = 20

	sem := semaphore.NewWeighted(int64(cfg.NumWorkers))
	var wg sync.WaitGroup

	processBatch := func(batch []types.ObjectIdentifier) {
		if len(batch) == 0 {
			return
		}
		wg.Add(1)
		if err := sem.Acquire(ctx, 1); err != nil {
			warn("Semaphore acquire failed: %v", err)
			wg.Done()
			return
		}

		go func(batch []types.ObjectIdentifier) {
			defer sem.Release(1)
			defer wg.Done()

			var resp *s3.DeleteObjectsOutput
			var err error

			if cfg.DryRun {
				deleted := make([]types.DeletedObject, len(batch))
				for i, o := range batch {
					deleted[i] = types.DeletedObject{Key: o.Key, VersionId: o.VersionId}
				}
				resp = &s3.DeleteObjectsOutput{Deleted: deleted}
			} else {
				input := &s3.DeleteObjectsInput{
					Bucket:                    aws.String(cfg.Bucket),
					Delete:                    &types.Delete{Objects: batch},
					BypassGovernanceRetention: aws.Bool(true),
				}
				for i := 0; i < cfg.Retries; i++ {
					ctxDel, cancel := context.WithTimeout(ctx, 60*time.Second)
					resp, err = cfg.Client.DeleteObjects(ctxDel, input)
					cancel()
					if err == nil {
						break
					}
					warn("Retry %d/%d for batch: %v", i+1, cfg.Retries, err)
					time.Sleep(time.Second * time.Duration(i+1))
				}
			}

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorCount += len(batch)
				if cfg.LiveErrors != nil {
					atomic.AddInt64(cfg.LiveErrors, int64(len(batch)))
				}
				failedObjects = append(failedObjects, batch...)
				if cfg.EventChan != nil {
					msg := err.Error()
					for _, o := range batch {
						emit(cfg.EventChan, DeletionEvent{
							Key: aws.ToString(o.Key), VersionId: aws.ToString(o.VersionId),
							Timestamp: time.Now(), Failed: true, Err: msg,
						})
					}
				}
			} else {
				deletedCount += len(resp.Deleted)
				errorCount += len(resp.Errors)
				if cfg.LiveDeleted != nil {
					atomic.AddInt64(cfg.LiveDeleted, int64(len(resp.Deleted)))
				}
				if cfg.LiveErrors != nil && len(resp.Errors) > 0 {
					atomic.AddInt64(cfg.LiveErrors, int64(len(resp.Errors)))
				}

				for _, e := range resp.Errors {
					failedObjects = append(failedObjects, types.ObjectIdentifier{Key: e.Key, VersionId: e.VersionId})
					if cfg.EventChan != nil {
						emit(cfg.EventChan, DeletionEvent{
							Key: aws.ToString(e.Key), VersionId: aws.ToString(e.VersionId),
							Timestamp: time.Now(), Failed: true, Err: aws.ToString(e.Message),
						})
					}
				}
				for _, obj := range resp.Deleted {
					if cfg.EventChan != nil {
						emit(cfg.EventChan, DeletionEvent{
							Key: aws.ToString(obj.Key), VersionId: aws.ToString(obj.VersionId),
							Timestamp: time.Now(),
						})
					}
				}
			}

			batchCount++
			samples = append(samples, throughputSample{T: time.Now(), Deleted: int64(deletedCount)})
			if len(samples) > rollingWindow {
				samples = samples[len(samples)-rollingWindow:]
			}
			if batchCount%10 == 0 {
				logger("Progress — Deleted: %d | Errors: %d", deletedCount, errorCount)
			}

			if err != nil || (resp != nil && len(resp.Errors) > 0) {
				consecutiveErrors++
			} else {
				consecutiveErrors = 0
			}
			if consecutiveErrors >= maxConsecutiveErrors {
				sleepDur := time.Duration(consecutiveErrors) * time.Second
				warn("High error rate; throttling for %v", sleepDur)
				time.Sleep(sleepDur)
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
	if cfg.EventChan != nil {
		close(cfg.EventChan)
	}
	if cfg.TotalObjects > 0 {
		logger("Final throughput: %.1f obj/s over %s",
			ThroughputFromSamples(samples), time.Since(cfg.StartTime).Truncate(time.Second))
	}
	return Result{
		DeletedCount:  deletedCount,
		ErrorCount:    errorCount,
		FailedObjects: failedObjects,
	}
}

// emit performs a non-blocking send so a slow consumer cannot stall deletions.
func emit(ch chan<- DeletionEvent, ev DeletionEvent) {
	select {
	case ch <- ev:
	default:
	}
}

// ThroughputFromSamples returns objects/sec across the given rolling window.
// Exposed for testing and reuse.
func ThroughputFromSamples(samples []throughputSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	first, last := samples[0], samples[len(samples)-1]
	dt := last.T.Sub(first.T).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(last.Deleted-first.Deleted) / dt
}

// NewSample is a small constructor used by tests and external consumers.
func NewSample(t time.Time, deleted int64) throughputSample {
	return throughputSample{T: t, Deleted: deleted}
}
